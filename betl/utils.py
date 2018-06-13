import sys
import shutil
import os
import datetime
import time
import ast
import json
import tempfile

from . import logger
from . import cli
from . conf import Conf
from . scheduler import Scheduler
from . import reporting
from . import fileIO


# TODO: split this up into different sets, particuarly so a read_src only
# run doesn't then go on to load the new model back out of the ss
def init(appConfigFile, runTimeParams, scheduleConfig=None):

    ###############
    # LOGGING OFF #
    ###############

    if scheduleConfig is None:

        scheduleConfig = getDetaulfScheduleConfig()

    # We can't log anything until we've checked the last execution ID #
    conf = Conf(appConfigFile, cli.processArgs(runTimeParams), scheduleConfig)

    # If we're running setup, we need to do this before checking the last
    # exec, because we're about to wipe it and start from scratch!
    # If it's successful, we log it lower down
    if conf.exe.RUN_SETUP:
        setupBetl(conf)

    # This sets the EXEC_ID in conf.state
    lastExecReport = setUpExecution(conf)

    conf.initialiseLogging()

    ##############
    # LOGGING ON #
    ##############

    logger.logExecutionStart(rerun=conf.state.RERUN_PREV_JOB)

    if conf.exe.RUN_SETUP:
        logger.logBetlSetupComplete()
        logger.logExecutionOverview(lastExecReport)
    else:
        if conf.state.RERUN_PREV_JOB:
            logger.logExecutionOverview(lastExecReport, rerun=True)
        else:
            logger.logExecutionOverview(lastExecReport)

    if conf.exe.DELETE_TMP_DATA:
        deleteTempoaryData(conf.ctrl.TMP_DATA_PATH)
    else:
        conf.state.populateFileNameMap(conf.ctrl.TMP_DATA_PATH)

    if conf.exe.READ_SRC:
        autoPopulateSrcSchemaDescriptions(conf)

    # Get the schema descriptions from schemas/, or from Google Sheets, if
    # the sheets have been edited since they were last saved to csv

    # Get the last modified dates of the versions saved to csv
    modTimesFile = open('schemas/lastModifiedTimes.txt', 'r')
    fileContent = modTimesFile.read()
    if fileContent == '':
        lastModifiedTimes = {}
    else:
        lastModifiedTimes = ast.literal_eval(fileContent)
    oneOrMoreLastModTimesChanged = False
    lastModTimesChanged = {}

    logger.logSchemaDescsLoad()

    # Check the last modified time of the Google Sheets
    schemaDescGsheets = conf.ctrl.getAllSchemaDescGSheetDatastores()

    for dbID in schemaDescGsheets:
        sheet = schemaDescGsheets[dbID]
        if (sheet.filename not in lastModifiedTimes
           or sheet.getLastModifiedTime() !=
           lastModifiedTimes[sheet.filename]):
            oneOrMoreLastModTimesChanged = True
            lastModTimesChanged[dbID] = True

    if oneOrMoreLastModTimesChanged:
        logger.logRefreshingSchemaDescsFromGsheets(len(lastModTimesChanged))
        for dbID in lastModTimesChanged:
            sheet = schemaDescGsheets[dbID]
            refreshSchemaDescCSVs(sheet, dbID)
            lastModifiedTimes[sheet.filename] = sheet.getLastModifiedTime()
        logger.logRefreshingSchemaDescsFromGsheets_done()

    if lastModTimesChanged:
        modTimesFile = open('schemas/lastModifiedTimes.txt', 'w')
        modTimesFile.write(json.dumps(lastModifiedTimes))

    if conf.exe.RUN_REBUILD_ALL or \
       conf.exe.RUN_REBUILD_SRC or \
       conf.exe.RUN_REBUILD_STG or \
       conf.exe.RUN_REBUILD_TRG or \
       conf.exe.RUN_REBUILD_SUM:
        logger.logPhysicalDataModelBuild()

    if conf.exe.RUN_REBUILD_ALL:
        for dataLayerID in conf.DATA_LAYERS:
            conf.getLogicalDataModel(dataLayerID).buildPhysicalDataModel()
    else:
        if conf.exe.RUN_REBUILD_SRC:
            conf.getLogicalDataModel('SRC').buildPhysicalDataModel()
        if conf.exe.RUN_REBUILD_STG:
            conf.getLogicalDataModel('STG').buildPhysicalDataModel()
        if conf.exe.RUN_REBUILD_TRG:
            conf.getLogicalDataModel('TRG').buildPhysicalDataModel()
        if conf.exe.RUN_REBUILD_SUM:
            conf.getLogicalDataModel('SUM').buildPhysicalDataModel()

    return conf


def run(conf):

    response = 'SUCCESS'

    if conf.exe.RUN_DATAFLOWS:
        scheduler = Scheduler(conf)
        response = scheduler.execute()

    if response == 'SUCCESS':

        checkDBsForSuperflousTables(conf)

        conf.ctrl.CTRL_DB.updateExecution(
            execId=conf.state.EXEC_ID,
            status='SUCCESSFUL',
            statusMessage='')

        logStr = ("\n" +
                  "THE JOB COMPLETED SUCCESSFULLY " +
                  "(the executions table has been updated)\n\n")

        reporting.generateExeSummary(
            conf=conf,
            execId=conf.state.EXEC_ID,
            bulkOrDelta=conf.exe.BULK_OR_DELTA,
            limitedData=conf.exe.DATA_LIMIT_ROWS)

        logger.logExecutionFinish(logStr)


def getDetaulfScheduleConfig():
    scheduleConfig = {
        'DEFAULT_EXTRACT': False,
        'SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],
        'DEFAULT_LOAD': False,
        'DEFAULT_SUMMARISE': False,
        'DEFAULT_DM_DATE': False,
        'TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],
        'EXTRACT_DFS': [],
        'TRANSFORM_DFS': [],
        'LOAD_DFS': [],
        'SUMMARISE_DFS': []
    }

    return scheduleConfig


def setUpExecution(conf):

    # Log in to the CTL DB and check the status of the last run
    lastExecDetails = getDetailsOfLastExecution(conf.ctrl.CTRL_DB)
    lastExecStatus = lastExecDetails['lastExecStatus']
    lastExecId = lastExecDetails['lastExecId']

    execId = None

    if lastExecStatus == 'NO_PREV_EXEC':
        execId = 1
    elif (lastExecStatus == 'RUNNING' and
          conf.exe.RUN_DATAFLOWS and
          not conf.exe.SKIP_WARNINGS):

        text = input(cli.LAST_EXE_STILL_RUNNING)
        sys.exit()

    elif (lastExecStatus != 'SUCCESSFUL' and
          conf.exe.RUN_DATAFLOWS and
          not conf.exe.SKIP_WARNINGS):

        text = input(cli.LAST_EXE_FAILED.format(status=lastExecStatus))
        if text.lower() != 'ignore':
            if conf.exe.RUN_SETUP or \
               conf.exe.RUN_REBUILD_ALL or \
               conf.exe.RUN_REBUILD_SRC or \
               conf.exe.RUN_REBUILD_STG or \
               conf.exe.RUN_REBUILD_TRG or \
               conf.exe.RUN_REBUILD_SUM:
                text = input(cli.CANT_RERUN_WITH_SETUP_OR_REBUILD)
                sys.exit()
            else:
                conf.state.RERUN_PREV_JOB = True
                execId = lastExecId
        else:
            execId = lastExecId + 1
    else:
        execId = lastExecId + 1

    conf.state.setExecID(execId)

    if not conf.state.RERUN_PREV_JOB:
        conf.ctrl.CTRL_DB.insertExecution(
            execId,
            conf.exe.BULK_OR_DELTA,
            conf.exe.DATA_LIMIT_ROWS)

    lastExecReport = {
        'lastExecId': lastExecId,
        'lastExecStatus': lastExecStatus,
        'execId': execId
    }
    return lastExecReport


def getDetailsOfLastExecution(ctlDB):

    lastExecRow = ctlDB.getLastExecution()

    lastExecDetails = {}
    if len(lastExecRow) > 0:  # in case it's the first execution!
        lastExecDetails = {'lastExecId': lastExecRow[0][0],
                           'lastExecStatus': lastExecRow[0][1]}
    else:
        lastExecDetails = {'lastExecId': None,
                           'lastExecStatus': 'NO_PREV_EXEC'}
    return lastExecDetails


def setupBetl(conf):
    conf.ctrl.CTRL_DB.dropAllCtlTables()
    archiveLogFiles(conf)
    setupSchemaDir(conf)
    conf.ctrl.CTRL_DB.createExecutionsTable()
    conf.ctrl.CTRL_DB.createFunctionsTable()
    conf.ctrl.CTRL_DB.createDataflowsTable()
    conf.ctrl.CTRL_DB.createStepsTable()


def archiveLogFiles(conf):

    timestamp = datetime.datetime.fromtimestamp(
        time.time()
    ).strftime('%Y%m%d%H%M%S')

    source = conf.ctrl.LOG_PATH
    dest = conf.ctrl.LOG_PATH + 'archive_' + timestamp + '/'

    if not os.path.exists(dest):
        os.makedirs(dest)

    files = os.listdir(source)
    for f in files:
        if f.find('.log') > -1:
            shutil.move(source+f, dest)


def setupSchemaDir(conf):

    dir = 'schemas/'
    shutil.rmtree(dir)
    os.makedirs(dir)
    open(dir + 'lastModifiedTimes.txt', 'a').close()


def deleteTempoaryData(tmpDataPath):

    path = tmpDataPath.replace('/', '')

    if (os.path.exists(path)):
        # `tempfile.mktemp` Returns an absolute pathname of a file that
        # did not exist at the time the call is made. We pass
        # dir=os.path.dirname(dir_name) here to ensure we will move
        # to the same filesystem. Otherwise, shutil.copy2 will be used
        # internally and the problem remains: we're still deleting the
        # folder when we come to recreate it
        tmp = tempfile.mktemp(dir=os.path.dirname(path))
        shutil.move(path, tmp)  # rename
        shutil.rmtree(tmp)  # delete
    os.makedirs(path)  # create the new folder


def checkDBsForSuperflousTables(conf):
    query = ("SELECT table_name FROM information_schema.tables " +
             "WHERE table_schema = 'public'")

    etlDBCursor = conf.data.getDatastore('ETL').cursor()
    etlDBCursor.execute(query)
    trgDBCursor = conf.data.getDatastore('TRG').cursor()
    trgDBCursor.execute(query)

    # query returns list of tuples, (<tablename>, )
    allTables = []
    allTables.extend([item[0] for item in etlDBCursor.fetchall()])
    allTables.extend([item[0] for item in trgDBCursor.fetchall()])

    dataModelTables = []
    for dataLayerID in conf.DATA_LAYERS:
        dataModelTables.extend(
            conf.getLogicalDataModel(dataLayerID).getListOfTables())

    superflousTableNames = []
    for tableName in allTables:
        if tableName not in dataModelTables:
            superflousTableNames.append(tableName)

    if len(superflousTableNames) > 0:
        conf.JOB_LOG.warn(
            logger.superflousTableWarning(',\n  '.join(superflousTableNames)))


def refreshSchemaDescCSVs(datastore, dbID):

    # Start by builing an array of all the releavnt worksheets from the DB's
    # schema desc gsheet.
    worksheets = []
    # It's important to call getWorksheets() again, rather than the worksheets
    # attribute, because we might have just replaced the SRC worksheets (if
    # we ran READ_SRC)
    # TODO: in fact, I'm increasingly writing my conf class methods to
    # check whether the id in question exists and handle accordingly,
    # I should probably work through all calls to conf and make them
    # consistently use these methods
    for gWorksheetTitle in datastore.getWorksheets():
        # skip any sheets that aren't prefixed with the DB (e.g. ETL or TRG)
        if gWorksheetTitle[0:4] == dbID + '.':
            worksheets.append(datastore.worksheets[gWorksheetTitle])

    dbSchemaDesc = {}

    logger.logLoadingDBSchemaDescsFromGsheets(dbID)

    for ws in worksheets:
        # Get the dataLayer, dataModel and table name from the worksheet title
        dataLayerID = ws.title[ws.title.find('.')+1:ws.title.rfind('.')]
        dataLayerID = dataLayerID[:dataLayerID.rfind('.')]
        dataModelID = ws.title[ws.title.find('.')+1:ws.title.rfind('.')]
        dataModelID = dataModelID[dataModelID.find('.')+1:]
        tableName = ws.title[ws.title.rfind('.')+1:]

        # If needed, create a new item in our db schema desc for
        # this data layer, and a new item in our dl schema desc for
        # this data model.
        # (there is a worksheet per table, many tables per data model,
        # and many data models per database)
        if dataLayerID not in dbSchemaDesc:
            dbSchemaDesc[dataLayerID] = {
                'dataLayerID': dataLayerID,
                'dataModelSchemas': {}
            }
        dlSchemaDesc = dbSchemaDesc[dataLayerID]
        if dataModelID not in dlSchemaDesc['dataModelSchemas']:
            dlSchemaDesc['dataModelSchemas'][dataModelID] = {
                'dataModelID': dataModelID,
                'tableSchemas': {}
            }
        dmSchemaDesc = dlSchemaDesc['dataModelSchemas'][dataModelID]

        # Create a new table schema description
        tableSchema = {
            'tableName': tableName,
            'columnSchemas': {}
        }

        # Pull out the column schema descriptions from the Google
        # worksheeet and restructure a little
        colSchemaDescsFromWS = ws.get_all_records()
        for colSchemaDescFromWS in colSchemaDescsFromWS:
            colName = colSchemaDescFromWS['Column Name']
            fkDimension = 'None'
            if 'FK Dimension' in colSchemaDescFromWS:
                fkDimension = colSchemaDescFromWS['FK Dimension']

            # append this column schema desc to our tableSchema object
            tableSchema['columnSchemas'][colName] = {
                'tableName':   tableName,
                'columnName':  colName,
                'dataType':    colSchemaDescFromWS['Data Type'],
                'columnType':  colSchemaDescFromWS['Column Type'],
                'fkDimension': fkDimension
            }

        # Finally, add the tableSchema to our data dataModel schema desc
        dmSchemaDesc['tableSchemas'][tableName] = tableSchema

    with open('schemas/dbSchemaDesc_' + dbID + '.txt', 'w') as file:
        file.write(json.dumps(dbSchemaDesc))


# TODO: this is a HORRIBLE mess of code and needs heavy refactoring!
def autoPopulateSrcSchemaDescriptions(conf):

    # First, loop through the ETL DB schema desc spreadsheet and delete
    # any worksheets prefixed ETL.SRC.

    ss = conf.ctrl.getSchemaDescGSheetDatastore('ETL').conn

    logger.logDeleteSrcSchemaDescWsFromSS()
    for ws in ss.worksheets():
        if ws.title.find('ETL.SRC.') == 0:
            ss.del_worksheet(ws)

    # Each source system will create a new data model within our SRC data
    # layer (within our ETL database)
    for srcSysID in conf.data.getListOfSrcSys():

        srcSysSchemas = {}

        srcSysDS = conf.data.getSrcSysDatastore(srcSysID)

        # The object we're going to build up before writing to the GSheet
        srcSysSchemas[srcSysID] = {
            'dataModelID': srcSysID,
            'tableSchemas': {}
        }

        if srcSysDS.datastoreType == 'POSTGRES':
            # one row in information_schema.columns for each column, spanning
            # multiple tables
            dbCursor = srcSysDS.conn.cursor()
            dbCursor.execute("SELECT * FROM information_schema.columns c " +
                             "WHERE c.table_schema = 'public' " +
                             "ORDER BY c.table_name")
            postgresSchema = dbCursor.fetchall()
            previousTableName = ''
            colSchemasFromPG = []
            for colSchemaFromPG in postgresSchema:
                currentTableName = ('src_' + srcSysID + '_' +
                                    colSchemaFromPG[2])
                if currentTableName == previousTableName:
                    colSchemasFromPG.append(colSchemaFromPG)
                else:
                    colSchemas = {}

                    for colSchemaFromPG in colSchemasFromPG:
                        colName = colSchemaFromPG[3]

                        colSchemas[colName] = {
                            'tableName': previousTableName,
                            'columnName': colName,
                            'dataType': colSchemaFromPG[7],
                            'columnType': 'Attribute',
                            'fkDimension': None
                        }

                    tableSchema = {
                        'tableName': previousTableName,
                        'columnSchemas': colSchemas
                    }
                    tableName = previousTableName
                    srcSysSchemas[srcSysID]['tableSchemas'][tableName] = \
                        tableSchema
                    colSchemasFromPG = []
                    colSchemasFromPG.append(colSchemaFromPG)
                    previousTableName = currentTableName

        elif srcSysDS.datastoreType == 'SQLITE':
            # one row in information_schema.columns for each column, spanning
            # multiple tables
            dbCursor = srcSysDS.conn.cursor()
            dbCursor.execute("SELECT * FROM sqlite_master c " +
                             "WHERE type = 'table' ")
            tables = dbCursor.fetchall()
            for table in tables:
                colSchemas = {}
                for row in srcSysDS.conn.execute(
                        "pragma table_info('" + table[1] + "')").fetchall():

                    colSchemas[row[1]] = {
                        'tableName': table[1],
                        'columnName': row[1],
                        'dataType': row[2],
                        'columnType': 'Attribute',
                        'fkDimension': None
                    }

                tableSchema = {
                    'tableName': table[1],
                    'columnSchemas': colSchemas
                }

                srcSysSchemas[srcSysID]['tableSchemas'][table[1]] = tableSchema

        elif (srcSysDS.datastoreType == 'FILESYSTEM' and
              srcSysDS.fileExt == '.csv'):
            # one src filesystem will contain 1+ files. Each file is a table,
            # obviously, and each will have a list of cols in the first row
            # Get all the files (in the root dir), then loop through each one
            files = []
            for (dirpath, dirnames, filenames) in os.walk(srcSysDS.path):
                files.extend(filenames)
                break  # Just the root
            for filename in files:
                if filename.find('.csv') > 0:
                    df = fileIO.readDataFromCsv(conf=conf,
                                                path=srcSysDS.path,
                                                filename=filename,
                                                sep=srcSysDS.delim,
                                                quotechar=srcSysDS.quotechar,
                                                isTmpData=False,
                                                getFirstRow=True)

                    cleanFileName = filename[0:len(filename)-4]

                    colSchemas = {}

                    for colName in df:
                        colSchemas[colName] = {
                            'tableName': tableName,
                            'columnName': colName,
                            'dataType': 'TEXT',
                            'columnType': 'Attribute',
                            'fkDimension': None
                        }

                    tableSchema = {
                        'tableName': tableName,
                        'columnSchemas': colSchemas
                    }

                    srcSysSchemas[srcSysID]['tableSchemas'][cleanFileName] = \
                        tableSchema

        elif (srcSysDS.datastoreType == 'GSHEET'):
            # one spreadsheet will contain multiple worksheets, each
            # worksheet containing one table. The top row is the column
            # headings.
            worksheets = srcSysDS.getWorksheets()
            for wsName in worksheets:
                colHeaders = worksheets[wsName].row_values(1)
                colSchemas = {}
                for colName in colHeaders:
                    if colName != '':
                        colSchemas[colName] = {
                            'tableName': wsName,
                            'columnName': colName,
                            'dataType': 'TEXT',
                            'columnType': 'Attribute',
                            'fkDimension': None
                        }

                tableSchema = {
                    'tableName': wsName,
                    'columnSchemas': colSchemas
                }
                srcSysSchemas[srcSysID]['tableSchemas'][wsName] = tableSchema

        elif (srcSysDS.datastoreType == 'EXCEL'):
            # one spreadsheet will contain multiple worksheets, each
            # worksheet containing one table. The top row is the column
            # headings.
            worksheets = srcSysDS.getWorksheets()
            for wsName in worksheets:
                colHeaders = worksheets[wsName]['1:1']
                colSchemas = {}
                for cell in colHeaders:
                    colName = cell.value
                    if colName != '':
                        colSchemas[colName] = {
                            'tableName': wsName,
                            'columnName': colName,
                            'dataType': 'TEXT',
                            'columnType': 'Attribute',
                            'fkDimension': None
                        }
                    else:
                        break

                tableSchema = {
                    'tableName': wsName,
                    'columnSchemas': colSchemas
                }
                srcSysSchemas[srcSysID]['tableSchemas'][wsName] = tableSchema

        else:
            raise ValueError("Failed to auto-populate SRC Layer schema desc:" +
                             " Source system type is " +
                             srcSysDS.datastoreType +
                             ". Stopping execution. We only " +
                             "deal with 'POSTGRES', 'FILESYSTEM' & 'GSHEET' " +
                             " source system types, so cannot auto-populate " +
                             "the ETL.SRC schemas for this source system")

        # Check we managed to find some kind of schema from the source system
        if (len(srcSysSchemas[srcSysID]['tableSchemas']) == 0):
            raise ValueError("Failed to auto-populate SRC Layer schema desc:" +
                             " we could not find any meta data in the src " +
                             "system with which to construct a schema " +
                             "description")
        else:
            for srcSysID in srcSysSchemas:
                logger.logAutoPopSchemaDescsFromSrc(srcSysID)
                tableSchemas = srcSysSchemas[srcSysID]['tableSchemas']
                for tableName in tableSchemas:
                    colSchemas = tableSchemas[tableName]['columnSchemas']

                    wsName = 'ETL.SRC.' + srcSysID + '.' + 'src_' + \
                        srcSysID.lower() + '_' + tableName

                    ws = ss.add_worksheet(title=wsName,
                                          rows=len(colSchemas)+1,
                                          cols=3)

                    # We build up our new GSheets table first, in memory,
                    # then write it all in one go.
                    cell_list = ws.range('A1:C'+str(len(colSchemas)+1))
                    rangeRowCount = 0
                    cell_list[rangeRowCount].value = 'Column Name'
                    cell_list[rangeRowCount+1].value = 'Data Type'
                    cell_list[rangeRowCount+2].value = 'Column Type'
                    rangeRowCount += 3
                    for col in colSchemas:
                        cell_list[rangeRowCount].value = \
                            colSchemas[col]['columnName']
                        cell_list[rangeRowCount+1].value = \
                            colSchemas[col]['dataType']
                        cell_list[rangeRowCount+2].value = \
                            colSchemas[col]['columnType']
                        rangeRowCount += 3

                    ws.update_cells(cell_list)
