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
from .dataLayer import SrcDataLayer
from .dataLayer import StgDataLayer
from .dataLayer import TrgDataLayer
from .dataLayer import SumDataLayer
from .conf import Conf
from .scheduler import Scheduler


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

    logger.logLogicalDataModelBuild()

    # Check the last modified time of the Google Sheets
    schemaDescGsheets = conf.ctrl.getSchemaDescGSheetDatastores()
    for dbID in schemaDescGsheets:
        sheet = schemaDescGsheets[dbID]
        if (sheet.filename not in lastModifiedTimes
           or sheet.lastModifiedTime != lastModifiedTimes[sheet.filename]):
            oneOrMoreLastModTimesChanged = True
            lastModTimesChanged[dbID] = True

    if oneOrMoreLastModTimesChanged:
        logger.logRefreshingSchemaDescsFromGsheets(len(lastModTimesChanged))
        for dbID in lastModTimesChanged:
            sheet = schemaDescGsheets[dbID]
            refreshSchemaDescCSVs(sheet, dbID)
            lastModifiedTimes[sheet.filename] = sheet.lastModifiedTime

    if lastModTimesChanged:
        modTimesFile = open('schemas/lastModifiedTimes.txt', 'w')
        modTimesFile.write(json.dumps(lastModifiedTimes))

    logicalDataModels = {}
    logicalDataModels['SRC'] = SrcDataLayer(conf)
    logicalDataModels['STG'] = StgDataLayer(conf)
    logicalDataModels['TRG'] = TrgDataLayer(conf)
    logicalDataModels['SUM'] = SumDataLayer(conf)
    conf.data.setLogicalDataModels(logicalDataModels)
    logger.logLogicalDataModelBuild_done(logicalDataModels)

    if conf.exe.RUN_REBUILD_ALL or \
       conf.exe.RUN_REBUILD_SRC or \
       conf.exe.RUN_REBUILD_STG or \
       conf.exe.RUN_REBUILD_TRG or \
       conf.exe.RUN_REBUILD_SUM:
        logger.logPhysicalDataModelBuild()

    if conf.exe.RUN_REBUILD_ALL:
        for dataModelID in logicalDataModels:
            logicalDataModels[dataModelID].buildPhysicalDataModel()
    else:
        if conf.exe.RUN_REBUILD_SRC:
            logicalDataModels['SRC'].buildPhysicalDataModel()
        if conf.exe.RUN_REBUILD_STG:
            logicalDataModels['STG'].buildPhysicalDataModel()
        if conf.exe.RUN_REBUILD_TRG:
            logicalDataModels['TRG'].buildPhysicalDataModel()
        if conf.exe.RUN_REBUILD_SUM:
            logicalDataModels['SUM'].buildPhysicalDataModel()

    checkDBsForSuperflousTables(conf)
    return conf


def run(conf):

    response = 'SUCCESS'

    if conf.exe.RUN_DATAFLOWS:
        scheduler = Scheduler(conf)
        response = scheduler.executeSchedule()

    if response == 'SUCCESS':
        conf.ctrl.CTRL_DB.updateExecutionInCtlTable(
            execId=conf.state.EXEC_ID,
            status='SUCCESSFUL',
            statusMessage='')
        logStr = ("\n" +
                  "THE JOB COMPLETED SUCCESSFULLY " +
                  "(the executions table has been updated)\n\n")
        logger.logExecutionFinish(logStr)


def getDetaulfScheduleConfig():
    scheduleConfig = {
        'DEFAULT_EXTRACT': False,
        'SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT': [],
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
        conf.ctrl.CTRL_DB.insertNewExecutionToCtlTable(
            execId,
            conf.exe.BULK_OR_DELTA)

    lastExecReport = {
        'lastExecId': lastExecId,
        'lastExecStatus': lastExecStatus,
        'execId': execId
    }
    return lastExecReport


def getDetailsOfLastExecution(ctlDB):

    lastExecRow = ctlDB.getLastExec()

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
    conf.ctrl.CTRL_DB.createSchedulesTable()


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
    for dataLayerID in conf.data.LOGICAL_DATA_MODELS:
        dataModelTables.extend(
            conf.data.LOGICAL_DATA_MODELS[dataLayerID].getListOfTables())

    superflousTableNames = []
    for tableName in allTables:
        if tableName not in dataModelTables:
            superflousTableNames.append(tableName)

    if len(superflousTableNames) > 0:
        conf.JOB_LOG.warn(
            logger.superflousTableWarning(',\n  '.join(superflousTableNames)))


def refreshSchemaDescCSVs(datastore, dbID):

    dbSchemaDesc = {}

    logger.logLoadingDBSchemaDescsFromGsheets(dbID)

    for gWorksheetTitle in datastore.worksheets:

        # skip any sheets that don't start, e.g. ETL. or TRG.
        if gWorksheetTitle[0:4] != dbID + '.':
            continue

        ws = datastore.worksheets[gWorksheetTitle]
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
