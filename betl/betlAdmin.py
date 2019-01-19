import sys
from betl.logger import cliText

def admin(commandLineArgs):

        cliArgs = processAdminCLIArgs(commandLineArgs)

        appConfig = ConfigObj(cliArgs['APP_CONFIG_FILE'])

        self.CONF = Conf(
            betl=None,
            appConfig=appConfig,
            scheduleConfig=None)

        if cliArgs['DELETE_TMP_DATA']:

            deleteTempoaryData()

        # TODO: I think this is set up to work if you want to populate the EXT
        # spreadsheet automatically based on the src system schemas, but not
        # sure how it can work if you want to do it manually...  Don't we need
        # to read the EXT spreadsheet into the data model and populate the src
        # table map?  I think I could probably add this to
        # refreshSchemaDescsFromGsheets
        if self.CONF.READ_SRC:

            autoPopulateExtSchemaDescGsheets()

            # Creates file in schema dir, srcTableNameMapping.txt
            autoPopulateExtSchemaDescGsheets()

        # TODO: check dependencies on this, too. Does it need read_src to finish?
        refreshSchemaDescsTxtFilesFromGsheets()

        if len(self.CONF.RUN_REBUILDS) > 0:
            buildPhysicalDWHSchemas(),


def processAdminCLIArgs(args):

    showHelp = False
    skipWarnings = False
    bulk = False
    delta = False
    isUnrecognisedArg = False

    if len(args) > 0 and args[0] == 'main.py':
        del args[0]

    appConfigFile = args[0]

    params = {

        'APP_CONFIG_FILE': appConfigFile,

        'LOG_LEVEL': None,

        'RERUN_PREV_JOB': False,

        'SKIP_WARNINGS': False,

        'BULK_OR_DELTA': 'NOT SET',

        'RUN_RESET': False,
        'READ_SRC': False,

        'RUN_REBUILDS': {},

        'RUN_EXTRACT': True,
        'RUN_TRANSFORM': True,
        'RUN_LOAD': True,
        'RUN_DM_LOAD': True,
        'RUN_FT_LOAD': True,
        'RUN_SUMMARISE': True,

        'RUN_TESTS': True,

        'WRITE_TO_ETL_DB': True,
        'DELETE_TMP_DATA': False,
        'DATA_LIMIT_ROWS': None,

        'RUN_DATAFLOWS': False,

        'FAIL_LAST_EXEC': False,

    }

    for arg in args:
        if arg == 'help':
            showHelp = True
        elif arg == 'reset':
            params['RUN_RESET'] = True
        elif arg == 'readsrc':
            params['READ_SRC'] = True
        elif arg == 'rebuildall':
            params['RUN_REBUILDS'] = {
                'EXT': True,
                'TRN': True,
                'BSE': True,
                'SUM': True}
        elif arg == 'rebuildext':
            params['RUN_REBUILDS']['EXT'] = True
        elif arg == 'rebuildtrn':
            params['RUN_REBUILDS']['TRN'] = True
        elif arg == 'rebuildbse':
            params['RUN_REBUILDS']['BSE'] = True
        elif arg == 'rebuildsum':
            params['RUN_REBUILDS']['SUM'] = True
        elif arg == 'cleartmpdata':
            params['DELETE_TMP_DATA'] = True
        elif arg == 'faillast':
            params['FAIL_LAST_EXEC'] = True
        else:
            isUnrecognisedArg = True
            unrecognisedArg = arg

    if isUnrecognisedArg and not showHelp:
        print(cliText.ARG_NOT_RECOGNISED.format(arg=unrecognisedArg))
        sys.exit()
    elif showHelp:
        print(cliText.HELP)
        sys.exit()
    else:
        if params['RUN_RESET']:
            if not skipWarnings:
                text = input(cliText.SETUP_WARNING)
                if text.lower() != 'y':
                    sys.exit()
                else:
                    print('')
        if params['READ_SRC']:
            if not skipWarnings:
                text = input(cliText.READ_SRC_WARNING)
                if text.lower() != 'y':
                    sys.exit()
                else:
                    print('')

    return params


#########################
# DELETE TEMPORARY DATA #
#########################

def deleteTempoaryData(betl):

    path = betl.CONF.TMP_DATA_PATH.replace('/', '')

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

    betl.LOG.logDeleteTemporaryDataEnd()


######################
# EXTRACT/SRC SCHEMA #
######################

def autoPopulateExtSchemaDescGsheets(betl):

    betl.LOG.logAutoPopExtSchemaDescGSheetsStart()

    # First, read the schemas of all the source system datastores
    # and build up the logical data model
    readSrcSystemSchemas(betl=betl)  # Stores datamodel in Conf object

    # Loop through the ETL DB schema desc spreadsheet and delete
    # any worksheets prefixed ETL.EXT.

    betl.LOG.logDeleteSrcSchemaDescWsFromSS()

    ss = betl.CONF.getSchemaDescDatastore('ETL').conn

    for ws in ss.worksheets():
        if ws.title.find('ETL.EXT.') == 0:
            ss.del_worksheet(ws)

    # Each source system will create a new dataset within our EXT data
    # layer (within our ETL database)

    for srcSysID in betl.CONF.SRC_SYSTEM_DETAILS:

        betl.LOG.logAddSrcSchemaDescToSS(srcSysID)

        tableSchemas = betl.CONF.SRC_SYSTEM_DATA_MODELS[srcSysID]['tableSchemas']

        for srcTableName in tableSchemas:

            extTableName = cleanTableName(srcTableName)

            colSchemas = tableSchemas[srcTableName]['columnSchemas']

            wsName = ('ETL.EXT.' + srcSysID + '.' +
                      srcSysID.lower() + '_' + extTableName)

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

    betl.LOG.logAutoPopExtSchemaDescsEnd()


# TODO: this is a HORRIBLE mess of code and needs heavy refactoring!
# (although it's a bit less bad than it was, having split it out a little)
def readSrcSystemSchemas(betl):

    for srcSysID in betl.CONF.SRC_SYSTEM_DETAILS:

        betl.LOG.logInitialiseSrcSysDatastore(
            datastoreID=srcSysID,
            datastoreType=betl.CONF.SRC_SYSTEM_DETAILS[srcSysID]['type'])

        srcSysDS = betl.CONF.getSrcSysDatastore(srcSysID)

        # The object we're going to build up before writing to the GSheet
        betl.CONF.SRC_SYSTEM_DATA_MODELS[srcSysID] = {
            'datasetID': srcSysID,
            'tableSchemas': {}
        }

        betl.LOG.logReadingSrcSysSchema(ssID=srcSysID)

        if srcSysDS.datastoreType == 'POSTGRES':
            # one row in information_schema.columns for each column,
            # spanning multiple tables
            dbCursor = srcSysDS.conn.cursor()
            dbCursor.execute(
                "SELECT * FROM information_schema.columns c " +
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
                    betl.CONF.SRC_SYSTEM_DATA_MODELS[srcSysID]['tableSchemas'][tableName] = tableSchema
                    colSchemasFromPG = []
                    colSchemasFromPG.append(colSchemaFromPG)
                    previousTableName = currentTableName

        elif srcSysDS.datastoreType == 'SQLITE':
            # one row in information_schema.columns for each column,
            # spanning multiple tables
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

                betl.CONF.SRC_SYSTEM_DATA_MODELS[srcSysID]['tableSchemas'][table[1]] = tableSchema

        elif (srcSysDS.datastoreType == 'FILESYSTEM' and
              srcSysDS.fileExt == '.csv'):

            # one src filesystem will contain 1+ files. Each file is a
            # table, obviously, and each will have a list of cols in the
            # first row. Get all the files (in the root dir), then loop
            # through each one
            files = []
            for (dirpath, dirnames, filenames) in os.walk(srcSysDS.path):
                files.extend(filenames)
                break  # Just the root

            for filename in files:

                if filename.find('.csv') > 0:
                    df = fileIO.readDataFromCsv(
                        fileNameMap=None,
                        path=srcSysDS.path,
                        filename=filename,
                        sep=srcSysDS.delim,
                        quotechar=srcSysDS.quotechar,
                        isTmpData=False,
                        getFirstRow=True)

                    cleanFName = filename[0:len(filename)-4]

                    colSchemas = {}

                    for colName in df:
                        colSchemas[colName] = {
                            'tableName': cleanFName,
                            'columnName': colName,
                            'dataType': 'TEXT',
                            'columnType': 'Attribute',
                            'fkDimension': None
                        }

                    tableSchema = {
                        'tableName': cleanFName,
                        'columnSchemas': colSchemas
                    }

                    betl.CONF.SRC_SYSTEM_DATA_MODELS[srcSysID]['tableSchemas'][cleanFName] = tableSchema

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
                betl.CONF.SRC_SYSTEM_DATA_MODELS[srcSysID]['tableSchemas'][wsName] = tableSchema

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
                    if colName != '' and colName is not None:
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
                betl.CONF.SRC_SYSTEM_DATA_MODELS[srcSysID]['tableSchemas'][wsName] = tableSchema

        else:
            raise ValueError(
                "Failed to auto-populate EXT Layer " +
                "schema desc: Source system type is " +
                srcSysDS.datastoreType +
                ". Stopping execution. We only " +
                "deal with 'POSTGRES', 'FILESYSTEM' & 'GSHEET' " +
                " source system types, so cannot auto-populate " +
                "the ETL.EXT schemas for this source system")

    # Check we managed to find some kind of schema from the source
    # system
    if (len(betl.CONF.SRC_SYSTEM_DATA_MODELS[srcSysID]['tableSchemas']) == 0):
        raise ValueError(
            "Failed to auto-populate EXT Layer schema desc:" +
            " we could not find any meta data in the src " +
            "system with which to construct a schema " +
            "description")


def populateSrcTableMap(betl):

    # Some data sources can provide us with table names
    # incompatible with Postgres (e.g. worksheet names in
    # Excel/GSheets). So we will create a mapping of actual names
    # to postgres names (which will be used for our EXT layer).
    # The mapping will be needed whenever we pull data
    # out of source. For simplicity, we'll do this for
    # all sources, even though some will always be the same.

    srcTableMap = {}

    for srcSysID in betl.CONF.SRC_SYSTEM_DETAILS:

        srcTableMap[srcSysID] = {}

        tableSchemas = betl.CONF.SRC_SYSTEM_DATA_MODELS[srcSysID]['tableSchemas']

        for srcTableName in tableSchemas:

            extTableName = srcSysID.lower() + '_' + cleanTableName(srcTableName)
            srcTableMap[srcSysID][extTableName] = srcTableName

    filePath = betl.CONF.SCHEMA_PATH + '/srcTableNameMapping.txt'
    with open(filePath, 'w+') as file:
        file.write(json.dumps(srcTableMap))

    betl.LOG.logPopulateSrcTableMapEnd()


#################################
# SCHEMA DESCRIPTION TEXT FILES #
#################################

def refreshSchemaDescsTxtFilesFromGsheets(betl):
    # Get the schema descriptions from the schema dir, or from Google Sheets,
    # if the sheets have been edited since they were last saved to csv, or if
    # the CSVs don't exist

    betl.LOG.logCheckLastModTimeOfSchemaDescGSheet()

    dbsToRefresh = {}
    lastModifiedTimes = {}

    lastModFilePath = betl.CONF.SCHEMA_PATH + '/lastModifiedTimes.txt'

    if os.path.exists(lastModFilePath):
        modTimesFile = open(lastModFilePath, 'r+')
        fileContent = modTimesFile.read()
        if fileContent != '':
            lastModifiedTimes = ast.literal_eval(fileContent)

    # Check the last modified time of the Google Sheets, and whether
    # the schemaDesc files even exist
    for dbId in betl.CONF.DWH_DATABASES_DETAILS:
        gSheet = betl.CONF.getSchemaDescDatastore(dbId)
        if (gSheet.filename not in lastModifiedTimes
           or not os.path.exists(betl.CONF.SCHEMA_PATH + '/dbSchemaDesc_' + dbId + '.txt')
           or gSheet.getLastModifiedTime() != lastModifiedTimes[gSheet.filename]):
            dbsToRefresh[dbId] = True

    if len(dbsToRefresh) > 0:
        betl.LOG.logRefreshingSchemaDescsFromGsheets(len(dbsToRefresh))
        for dbId in dbsToRefresh:
            gSheet = betl.CONF.getSchemaDescDatastore(dbId)

            # -----
            # Start by builing an array of all the releavnt worksheets from the
            # DB's schema desc gsheet.
            worksheets = []
            # It's important to call getWorksheets() again, rather than the
            # worksheets attribute, because we might have just replaced the SRC
            # worksheets (if we ran READ_SRC)
            # TODO: in fact, I'm increasingly writing my conf class methods to
            # check whether the id in question exists and handle accordingly,
            # I should probably work through all calls to conf and make them
            # consistently use these methods
            for gWorksheetTitle in gSheet.getWorksheets():
                # skip README sheets, and any sheets prefixed with "IGN."
                if gWorksheetTitle[0:4] != 'IGN.' and gWorksheetTitle.lower() != 'readme':
                    worksheets.append(gSheet.worksheets[gWorksheetTitle])

            dbSchemaDesc = {}

            betl.LOG.logLoadingDBSchemaDescsFromGsheetsStart(dbId)

            for ws in worksheets:
                # Get the dataLayer, dataset and table name from the worksheet
                # title. The TRG schmea desc spreadsheet only needs to code
                # worksheets with the datalayer (beacuse there's only one dataset
                # per datalayer: <dataLayerID>.<tableName>). We default the
                # datasetID to the dataLayerID
                # The ETL worksheet names are:
                # <dataLayerID>.<datasetID>.<tableName>
                if dbId == 'TRG':
                    dataLayerID = ws.title[0:ws.title.find('.')]
                    datasetID = dataLayerID
                    tableName = ws.title[ws.title.rfind('.')+1:]
                elif dbId == 'ETL':
                    dataLayerID = ws.title[0:ws.title.find('.')]
                    datasetID = ws.title[ws.title.find('.')+1:ws.title.rfind('.')]
                    tableName = ws.title[ws.title.rfind('.')+1:]

                # If needed, create a new item in our db schema desc for
                # this data layer, and a new item in our dl schema desc for
                # this data model.
                # (there is a worksheet per table, many tables per data model,
                # and many data models per database)
                if dataLayerID not in dbSchemaDesc:
                    dbSchemaDesc[dataLayerID] = {
                        'dataLayerID': dataLayerID,
                        'datasetSchemas': {}
                    }
                dlSchemaDesc = dbSchemaDesc[dataLayerID]
                if datasetID not in dlSchemaDesc['datasetSchemas']:
                    dlSchemaDesc['datasetSchemas'][datasetID] = {
                        'datasetID': datasetID,
                        'tableSchemas': {}
                    }
                datasetSchemaDesc = dlSchemaDesc['datasetSchemas'][datasetID]

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

                # Finally, add the tableSchema to our data dataset schema desc
                datasetSchemaDesc['tableSchemas'][tableName] = tableSchema

            with open(betl.CONF.SCHEMA_PATH + '/dbSchemaDesc_' + dbId + '.txt', 'w') as file:
                file.write(json.dumps(dbSchemaDesc))
            # -----
            lastModifiedTimes[gSheet.filename] = gSheet.getLastModifiedTime()

        modTimesFile = open(betl.CONF.SCHEMA_PATH + '/lastModifiedTimes.txt', 'w')
        modTimesFile.write(json.dumps(lastModifiedTimes))

        betl.LOG.logRefreshingSchemaDescsFromGsheetsEnd()


########################
# PHYSICAL DWH SCHEMAS #
########################

def buildPhysicalDWHSchemas(betl):

    betl.LOG.logBuildPhysicalDWHSchemaStart()

    for dlId in betl.CONF.RUN_REBUILDS:
        betl.CONF.DWH_LOGICAL_SCHEMAS[dlId].buildPhysicalSchema()

    betl.LOG.logBuildPhysicalDWHSchemaEnd()


###################################
# Wizard for new project creation #
###################################

def createNewBETLProject(params=None):
    if params is None:
        setup = betl.setup.setupUtils.createNewBETLProject_withUserInput()
    else:
        setup = betl.setup.setupUtils.createNewBETLProject(params)

    return setup
