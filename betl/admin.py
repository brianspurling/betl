import os
import json

from configobj import ConfigObj

from betl.ConfClass import Conf
from betl.io import fileIO
from betl.setup import SetupClass
from betl.setup import fileSetup


def admin(appDirectory,
          appConfigFileName,
          createNewProject=False,
          reset=False,
          deleteTempData=False,
          readSrc=False,
          refreshSchemaDescTextFiles=False,
          runRebuilds=False):

        appDirectory = os.path.expanduser(appDirectory)

        config = {
            'appDirectory': appDirectory,
            'appConfig': ConfigObj(appDirectory + appConfigFileName),
            'scheduleConfig': None,
            'isAdmin': True,
            'isAirflow': False}

        conf = Conf(config)

        if createNewProject:

            createNewBETLProject()

        if reset:

            conf.log('logResetStart')

            fileSetup.archiveLogFiles(conf)
            fileSetup.createReportsDir(conf)
            fileSetup.createSchemaDir(conf)

            conf.log('logResetEnd')

        if deleteTempData:

            fileSetup.deleteTemporaryData(conf)

        # TODO: I think this is set up to work if you want to populate the EXT
        # spreadsheet automatically based on the src system schemas, but not
        # sure how it can work if you want to do it manually...  Don't we need
        # to read the EXT spreadsheet into the data model and populate the src
        # table map?  I think I could probably add this to
        # refreshSchemaDescsFromGsheets
        if readSrc:

            srcSystemDataModels = readSrcSystemSchemas(conf)

            autoPopulateExtSchemaDescGsheets(conf, srcSystemDataModels)

            refreshSchemaDescsTxtFilesFromGsheets(conf)

            conf.constructLogicalDWHSchemas()

            for x in conf.DWH_LOGICAL_SCHEMAS:
                print(conf.DWH_LOGICAL_SCHEMAS[x])

        if refreshSchemaDescTextFiles:

            refreshSchemaDescsTxtFilesFromGsheets(conf)

            conf.constructLogicalDWHSchemas()

        if runRebuilds:

            conf.constructLogicalDWHSchemas()

            buildPhysicalDWHSchemas(conf)


######################
# EXTRACT/SRC SCHEMA #
######################

def autoPopulateExtSchemaDescGsheets(conf, srcSystemDataModels):

    conf.log('logAutoPopExtSchemaDescGSheetsStart')

    # Loop through the ETL DB schema desc spreadsheet and delete
    # any worksheets prefixed ETL.EXT.

    conf.log('logDeleteSrcSchemaDescWsFromSS')

    ss = conf.getSchemaDescDatastore('ETL').conn

    for ws in ss.worksheets():
        if ws.title.find('ETL.EXT.') == 0:
            ss.del_worksheet(ws)

    # Each source system will create a new dataset within our EXT data
    # layer (within our ETL database)

    for srcSysID in conf.SRC_SYSTEM_DETAILS:

        conf.log('logAddSrcSchemaDescToSS', ssID=srcSysID)

        tableSchemas = srcSystemDataModels[srcSysID]['tableSchemas']

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

    conf.log('logAutoPopExtSchemaDescsEnd')


# Connect to each source system, read the schema, return as dictionary:
#   dataModes < src systems < datasets (same as src system ID) < tables < cols
def readSrcSystemSchemas(conf):

    # The object we're going to build up before writing to the GSheet
    srcSystemDataModels = {}

    for srcSysID in conf.SRC_SYSTEM_DETAILS:

        conf.log(
            'logInitialiseSrcSysDatastore',
            datastoreID=srcSysID,
            datastoreType=conf.SRC_SYSTEM_DETAILS[srcSysID]['type'])

        srcSystemDataModels[srcSysID] = {
            'datasetID': srcSysID,
            'tableSchemas': {}
        }

        srcSysDS = conf.getSrcSysDatastore(srcSysID)

        # for convenience
        tblSchemas = srcSystemDataModels[srcSysID]['tableSchemas']

        conf.log('logReadingSrcSysSchema', ssID=srcSysID)

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
                        'cleanedTableName': cleanTableName(previousTableName),
                        'columnSchemas': colSchemas
                    }
                    tableName = previousTableName
                    tblSchemas[tableName] = tableSchema
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
                    'cleanedTableName': cleanTableName(table[1]),
                    'columnSchemas': colSchemas
                }

                tblSchemas[table[1]] = tableSchema

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
                        conf=conf,
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
                        'cleanedTableName': cleanTableName(cleanFName),
                        'columnSchemas': colSchemas
                    }

                    tblSchemas[cleanFName] = tableSchema

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
                    'cleanedTableName': cleanTableName(wsName),
                    'columnSchemas': colSchemas
                }
                tblSchemas[wsName] = tableSchema

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
                    'cleanedTableName': cleanTableName(wsName),
                    'columnSchemas': colSchemas
                }
                tblSchemas[wsName] = tableSchema

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
    if (len(srcSystemDataModels[srcSysID]['tableSchemas']) == 0):
        raise ValueError(
            "Failed to auto-populate EXT Layer schema desc:" +
            " we could not find any meta data in the src " +
            "system with which to construct a schema " +
            "description")

    return srcSystemDataModels


#################################
# SCHEMA DESCRIPTION TEXT FILES #
#################################

def refreshSchemaDescsTxtFilesFromGsheets(conf):
    # Get the schema descriptions from the schema dir, or from Google Sheets,

    conf.log('logRefreshingSchemaDescsTxtFilesFromGsheetsStart')

    for dbId in conf.DWH_DATABASES_DETAILS:
        gSheet = conf.getSchemaDescDatastore(dbId)

        # -----
        # Start by builing an array of all the releavnt worksheets from the
        # DB's schema desc gsheet.
        worksheets = []
        # It's important to call getWorksheets() again, rather than the
        # worksheets attribute, because we might have just replaced the SRC
        # worksheets (if we ran READ_SRC)
        for gWorksheetTitle in gSheet.getWorksheets():
            # skip README sheets, and any sheets prefixed with "IGN."
            if (gWorksheetTitle[0:4] != 'IGN.' and
                    gWorksheetTitle.lower() != 'readme'):
                worksheets.append(gSheet.worksheets[gWorksheetTitle])

        dbSchemaDesc = {}

        conf.log('logLoadingDBSchemaDescsFromGsheets', dbId=dbId)

        for ws in worksheets:
            # Get the dataLayer, dataset and table name from the worksheet
            # title. The TRG schmea desc spreadsheet only needs to code
            # worksheets with the datalayer (beacuse there's only one
            # dataset per datalayer: <dataLayerID>.<tableName>). We default
            # the datasetID to the dataLayerID
            # The ETL worksheet names are:
            # <dataLayerID>.<datasetID>.<tableName>
            if dbId == 'TRG':
                dataLayerID = ws.title[0:ws.title.find('.')]
                dsID = dataLayerID
                tableName = ws.title[ws.title.rfind('.')+1:]
            elif dbId == 'ETL':
                dataLayerID = ws.title[0:ws.title.find('.')]
                dsID = ws.title[ws.title.find('.')+1:ws.title.rfind('.')]
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
            if dsID not in dlSchemaDesc['datasetSchemas']:
                dlSchemaDesc['datasetSchemas'][dsID] = {
                    'datasetID': dsID,
                    'tableSchemas': {}
                }
            datasetSchemaDesc = dlSchemaDesc['datasetSchemas'][dsID]

            # Create a new table schema description

            # For the EXT layer, some data sources can provide us with table
            # name incompatible with Postgres (e.g. worksheet names in Excel/
            # GSheets). The table names in EXT schema description GSheet needs
            # to match source exactly, otherwise the default extract won't run
            # (and, of course, if autoPopulateExtSchemaDescGsheets() has been
            # used, then then the table names will match by default). This
            # leaves us with a problem, because we can't then use these table
            # names in our Postgres EXT layer. So use a cleanTableName function
            # and store along with the src table name

            if dataLayerID == 'EXT':
                cleanedTableName = cleanTableName(tableName)
            else:
                cleanedTableName = ''

            tableSchema = {
                'tableName': tableName,
                'cleanTableName': cleanedTableName,
                'schema': conf.DWH_DATABASES_DETAILS[dbId]['schema'],
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
                    'schema':  conf.DWH_DATABASES_DETAILS[dbId]['schema'],
                    'tableName':   tableName,
                    'columnName':  colName,
                    'dataType':    colSchemaDescFromWS['Data Type'],
                    'columnType':  colSchemaDescFromWS['Column Type'],
                    'fkDimension': fkDimension
                }

            # Finally, add the tableSchema to our data dataset schema desc
            datasetSchemaDesc['tableSchemas'][tableName] = tableSchema

        filename = conf.SCHEMA_PATH + '/dbSchemaDesc_' + dbId + '.txt'
        with open(filename, 'w') as file:
            file.write(json.dumps(dbSchemaDesc))

        conf.log('logRefreshingSchemaDescsTxtFilesFromGsheetsEnd')


########################
# PHYSICAL DWH SCHEMAS #
########################

def buildPhysicalDWHSchemas(conf):

    conf.log('logBuildPhysicalDWHSchemaStart')

    for dlId in conf.DWH_LOGICAL_SCHEMAS:
        conf.DWH_LOGICAL_SCHEMAS[dlId].buildPhysicalSchema()

    conf.log('logBuildPhysicalDWHSchemaEnd')


###################################
# Wizard for new project creation #
###################################

def createNewBETLProject():

    print('\nBefore setting up a new BETL project you will need a Google \n' +
          'API key file in your current directory. Get your file from ' +
          'Google API Console. \nYou also need a Postgres server with the ' +
          'u/n & p/w for the default postgres database.' +
          '\n\nTo select default values, press enter.')

    params = getParamsFromUserInput()

    setup = createProject(params)

    print("\nWe've done as much as we can. Now you need to: \n" +
          "  - replace the example source systems in appConfig.ini with " +
          "your actual source system(s) config \n" +
          "  - run  `python main.py readsrc rebuildall bulk run` , " +
          "which will: \n" +
          "      - auto-populate the schema descriptions for your SRC " +
          "datalayer \n" +
          "      - create the physical data model in your ETL and TRG " +
          "databases \n" +
          "      - run the default bulk extract.\n" +
          "Then you can explore the source data in your ETL database, " +
          "define your \n" +
          "TRG data layer schema in Google Sheets, and start writing " +
          "your data pipeline\n\n")

    return setup


def getParamsFromUserInput():

    params = {}

    # Values needed for appConfig.ini

    params['dwhId'] = input('\n* Shortname for the data warehouse >> ')

    params['apiKeyFilename'] = input('\n* Google API Key filename >> ')
    if params['apiKeyFilename'] is None or params['apiKeyFilename'] == '':
        params['apiKeyFilename'] = input('You must provide a filename! >> ')

    params['googleAccount'] = input('\n* The Google Account you will use to ' +
                                    'manage the DWH >> ')
    if params['googleAccount'] is None or params['googleAccount'] == '':
        params['googleAccount'] = input('You must provide a Google ' +
                                        'account! >> ')

    params['adminPostgresUsername'] = input('\n* Admin Postgres server ' +
                                            'username >> ')
    if (params['adminPostgresUsername'] is None or
            params['adminPostgresUsername'] == ''):
        params['adminPostgresUsername'] = input('You must provide a ' +
                                                'Postgres admin username! >> ')

    params['adminPostgresPassword'] = input('\n* Admin Postgres server ' +
                                            'password >> ')
    params['appRootPath'] = input('\nApp root path >> ')
    params['tmpDataPath'] = input('\nTemp data path >> ')
    params['srcDataPath'] = input('\nSource data path >> ')
    params['reportsPath'] = input('\nReports path >> ')
    params['logsPath'] = input('\nLogs path >> ')
    params['schemaPath'] = input('\nSchema path >> ')
    params['ctlDBHostName'] = input('\nControl DB - host name >> ')
    params['ctlDBName'] = input('\nControl DB - database name >> ')
    params['ctlDBUsername'] = input('\nControl DB - username >> ')
    params['ctlDBPassword'] = input('\nControl DB - password >> ')
    params['etlGSheetTitle'] = input('\nSchema description - ETL Google ' +
                                     'Sheets title >> ')
    params['trgGSheetTitle'] = input('\nSchema descriptions - TRG Google ' +
                                     'Sheets title >> ')
    params['etlDBHostName'] = input('\nETL DB - host name >> ')
    params['etlDBName'] = input('\nETL DB - database name >> ')
    params['etlDBUsername'] = input('\nETL DB - username >> ')
    params['etlDBPassword'] = input('\nETL DB - password >> ')
    params['trgDBHostName'] = input('\nTRG DB - host name >> ')
    params['trgDBName'] = input('\nTRG DB - database name >> ')
    params['trgDBUsername'] = input('\nTRG DB - username >> ')
    params['trgDBPassword'] = input('\nTRG DB - password >> ')
    params['defaultRowsGSheetTitle'] = input('\nDefault Rows - Google ' +
                                             'Sheets title >> ')
    params['mdmGSheetTitle'] = input('\nMaster Data Mappings - Google ' +
                                     'Sheets title >> ')

    # Whether we create the directories and default files

    params['createDirectories'] = input('\nCreate your directories? (Y/N) >> ')
    params['createGitignoreFile'] = input('\nCreate your .gitignore file? ' +
                                          '(Y/N) >> ')
    params['createAppConfigFile'] = input('\nCreate your appConfig.ini ' +
                                          'file? (Y/N) >> ')
    params['createMainScript'] = input('\nCreate your main.py script? ' +
                                       '(Y/N) >> ')
    params['createExampleDataflow'] = input('\nCreate an example dataflow ' +
                                            'script? (Y/N) >> ')

    # Whether we create the database and GSheets

    params['createDatabases'] = input('\nCreate three Postgres databases: ' +
                                      'Ctrl, ETL and Target? (Y/N) >> ')
    params['createSchemaDescGSheets'] = input('\nCreate two empty schema ' +
                                              'description Google Sheets: ' +
                                              'ETL and TRG? (Y/N) >> ')
    params['createMDMGsheet'] = input('\nCreate an empty MDM (master data ' +
                                      'mapping) Google Sheet: (Y/N) >> ')
    params['createDefaultRowsGsheet'] = input('\nCreate an empty default ' +
                                              'rows Google Sheet: (Y/N) >> ')

    return params


def createProject(params):

    setup = SetupClass.Setup()

    # Set the class attributes

    param = ''
    if 'dwhId' in params:
        param = params['dwhId']
    setup.setDwhId(param)

    param = ''
    if 'apiKeyFilename' in params:
        param = params['apiKeyFilename']
    setup.setGoogleAPIKeyFilename(param)

    param = ''
    if 'googleAccount' in params:
        param = params['googleAccount']
    setup.setGoogleAccount(param)

    param = ''
    if 'adminPostgresUsername' in params:
        param = params['adminPostgresUsername']
    setup.setAdminPostgresUsername(param)

    param = ''
    if 'adminPostgresPassword' in params:
        param = params['adminPostgresPassword']
    setup.setAdminPostgresPassword(param)

    param = ''
    if 'appRootPath' in params:
        param = params['appRootPath']
    setup.setAppRootPath(param)

    param = ''
    if 'tmpDataPath' in params:
        param = params['tmpDataPath']
    setup.setTmpDataPath(param)

    param = ''
    if 'srcDataPath' in params:
        param = params['srcDataPath']
    setup.setSrcDataPath(param)

    param = ''
    if 'reportsPath' in params:
        param = params['reportsPath']
    setup.setReportsPath(param)

    param = ''
    if 'logsPath' in params:
        param = params['logsPath']
    setup.setLogsPath(param)

    param = ''
    if 'schemaPath' in params:
        param = params['schemaPath']
    setup.setSchemaPath(param)

    param = ''
    if 'ctlDBHostName' in params:
        param = params['ctlDBHostName']
    setup.setCtlDBHostName(param)

    param = ''
    if 'ctlDBName' in params:
        param = params['ctlDBName']
    setup.setCtlDBName(param)

    param = ''
    if 'ctlDBUsername' in params:
        param = params['ctlDBUsername']
    setup.setCtlDBUsername(param)

    param = ''
    if 'ctlDBPassword' in params:
        param = params['ctlDBPassword']
    setup.setCtlDBPassword(param)

    param = ''
    if 'etlGSheetTitle' in params:
        param = params['etlGSheetTitle']
    setup.setSchemaDescETLGsheetTitle(param)

    param = ''
    if 'trgGSheetTitle' in params:
        param = params['trgGSheetTitle']
    setup.setSchemaDescTRGGsheetTitle(param)

    param = ''
    if 'etlDBHostName' in params:
        param = params['etlDBHostName']
    setup.setETLDBHostName(param)

    param = ''
    if 'etlDBName' in params:
        param = params['etlDBName']
    setup.setETLDBName(param)

    param = ''
    if 'etlDBUsername' in params:
        param = params['etlDBUsername']
    setup.setETLDBUsername(param)

    param = ''
    if 'etlDBPassword' in params:
        param = params['etlDBPassword']
    setup.setETLDBPassword(param)

    param = ''
    if 'trgDBHostName' in params:
        param = params['trgDBHostName']
    setup.setTRGDBHostName(param)

    param = ''
    if 'trgDBName' in params:
        param = params['trgDBName']
    setup.setTRGDBName(param)

    param = ''
    if 'trgDBUsername' in params:
        param = params['trgDBUsername']
    setup.setTRGDBUsername(param)

    param = ''
    if 'trgDBPassword' in params:
        param = params['trgDBPassword']
    setup.setTRGDBPassword(param)

    param = ''
    if 'defaultRowsGSheetTitle' in params:
        param = params['defaultRowsGSheetTitle']
    setup.setDefaultRowsGSheetTitle(param)

    param = ''
    if 'mdmGSheetTitle' in params:
        param = params['mdmGSheetTitle']
    setup.setMDMGSheetTitle(param)

    # Set up BETL

    param = ''
    if 'createDirectories' in params:
        param = params['createDirectories']
    setup.createDirectories(param)

    param = ''
    if 'createGitignoreFile' in params:
        param = params['createGitignoreFile']
    setup.createGitignoreFile(param)

    param = ''
    if 'createAppConfigFile' in params:
        param = params['createAppConfigFile']
    setup.createAppConfigFile(param)

    param = ''
    if 'createMainScript' in params:
        param = params['createMainScript']
    setup.createMainScript(param)

    param = ''
    if 'createExampleDataflow' in params:
        param = params['createExampleDataflow']
    setup.createExampleDataflow(param)

    param = ''
    if 'createDatabases' in params:
        param = params['createDatabases']
    setup.createDatabases(param)

    param = ''
    if 'createSchemaDescGSheets' in params:
        param = params['createSchemaDescGSheets']
    setup.createSchemaDescGSheets(param)

    param = ''
    if 'createMDMGsheet' in params:
        param = params['createMDMGsheet']
    setup.createMDMGsheet(param)

    param = ''
    if 'createDefaultRowsGsheet' in params:
        param = params['createDefaultRowsGsheet']
    setup.createDefaultRowsGsheet(param)

    return setup


def cleanTableName(tableName_src):
    tableName = tableName_src
    tableName = tableName.replace(' ', '_')
    tableName = tableName.replace('(', '')
    tableName = tableName.replace(')', '')
    tableName = tableName.replace('-', '')
    tableName = tableName.lower()
    return tableName


# admin(appDirectory="~/git/pngi/pngi/",
#       appConfigFileName="appConfig.ini",
#       createNewProject=False,
#       reset=False,
#       deleteTempData=False,
#       readSrc=False,
#       refreshSchemaDescTextFiles=True,
#       runRebuilds=False)
