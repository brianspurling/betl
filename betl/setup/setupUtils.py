from .SetupClass import Setup


def createNewBETLProject(params):

    setup = Setup()

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


def createNewBETLProject():

    print('\nBefore setting up a new BETL project you will need a Google \n' +
          'API key file in your current directory. Get your file from ' +
          'Google API Console. \nYou also need a Postgres server with the ' +
          'u/n & p/w for the default postgres database.' +
          '\n\nTo select default values, press enter.')

    params = getParamsFromUserInput()

    setup = reateNewBETLProject(params)

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

    params['googleAccount'] = input('\n* The Google Account you will use to manage the DWH >> ')
    if params['googleAccount'] is None or params['googleAccount'] == '':
        params['googleAccount'] = input('You must provide a Google account! >> ')

    params['adminPostgresUsername'] = input('\n* Admin Postgres server username >> ')
    if params['adminPostgresUsername'] is None or params['adminPostgresUsername'] == '':
        params['adminPostgresUsername'] = input('You must provide a Postgres admin username! >> ')

    params['adminPostgresPassword'] = input('\n* Admin Postgres server password >> ')
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
    params['etlGSheetTitle'] = input('\nSchema description - ETL Google Sheets title >> ')
    params['trgGSheetTitle'] = input('\nSchema descriptions - TRG Google Sheets title >> ')
    params['etlDBHostName'] = input('\nETL DB - host name >> ')
    params['etlDBName'] = input('\nETL DB - database name >> ')
    params['etlDBUsername'] = input('\nETL DB - username >> ')
    params['etlDBPassword'] = input('\nETL DB - password >> ')
    params['trgDBHostName'] = input('\nTRG DB - host name >> ')
    params['trgDBName'] = input('\nTRG DB - database name >> ')
    params['trgDBUsername'] = input('\nTRG DB - username >> ')
    params['trgDBPassword'] = input('\nTRG DB - password >> ')
    params['defaultRowsGSheetTitle'] = input('\nDefault Rows - Google Sheets title >> ')
    params['mdmGSheetTitle'] = input('\nMaster Data Mappings - Google Sheets title >> ')

    # Whether we create the directories and default files

    params['createDirectories'] = input('\nCreate your directories? (Y/N) >> ')
    params['createGitignoreFile'] = input('\nCreate your .gitignore file? (Y/N) >> ')
    params['createAppConfigFile'] = input('\nCreate your appConfig.ini file? (Y/N) >> ')
    params['createMainScript'] = input('\nCreate your main.py script? (Y/N) >> ')
    params['createExampleDataflow'] = input('\nCreate an example dataflow script? (Y/N) >> ')

    # Whether we create the database and GSheets

    params['createDatabases'] = input('\nCreate three Postgres databases: Ctrl, ETL and Target? (Y/N) >> ')
    params['createSchemaDescGSheets'] = input('\nCreate two empty schema description Google Sheets: ETL and TRG? (Y/N) >> ')
    params['createMDMGsheet'] = input('\nCreate an empty MDM (master data mapping) Google Sheet: (Y/N) >> ')
    params['createDefaultRowsGsheet'] = input('\nCreate an empty default rows Google Sheet: (Y/N) >> ')

    return params
