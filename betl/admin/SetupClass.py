class Setup():

    # To keep the code maintainable, we have divied up the class' functions
    # across multiple modules.  So we must import these all into the class.
    # This module, with the Setup() class, contains all the basic "set"
    # functions  where info is input into the class, plus the control functions
    # that run through the steps

    from fileSetup import (createDirectories,
                           createGitignoreFile,
                           createAppConfigFile,
                           createMainScript,
                           createExampleDataflow)

    from dbSetup import (createDatabases)

    from gsheetSetup import (createSchemaDescGSheets,
                             createMDMGsheet,
                             createDefaultRowsGsheet)

    def __init__(self):

        self.GOOGLE_API_SCOPE = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

        self.DWH_ID = 'DWH'

        # These three must be set by the user - no default value possible
        self.GOOGLE_API_KEY_FILENAME = None
        self.GOOGLE_ACCOUNT = None
        self.ADMIN_POSTGRES_USERNAME = None

        self.ADMIN_POSTGRES_PASSWORD = ''

        self.APP_ROOT_PATH = '.'
        self.TMP_DATA_PATH = self.APP_ROOT_PATH + '/' + 'tmp_data'
        self.SRC_DATA_PATH = self.APP_ROOT_PATH + '/' + 'src_data'
        self.REPORTS_PATH = self.APP_ROOT_PATH + '/' + 'reports'
        self.LOG_PATH = self.APP_ROOT_PATH + '/' + 'logs'
        self.SCHEMA_PATH = self.APP_ROOT_PATH + '/' + 'schema'

        self.SCHEMA_DESC_ETL_GSHEET_TITLE = self.DWH_ID + ' - ETL DB SCHEMA'
        self.SCHEMA_DESC_TRG_GSHEET_TITLE = self.DWH_ID + ' - TRG DB SCHEMA'

        self.ETL_DB_HOST_NAME = 'localhost'
        self.ETL_DB_NAME = self.DWH_ID.lower() + '_etl'
        self.ETL_DB_USERNAME = self.ADMIN_POSTGRES_USERNAME
        self.ETL_DB_PASSWORD = self.ADMIN_POSTGRES_PASSWORD

        self.TRG_DB_HOST_NAME = 'localhost'
        self.TRG_DB_NAME = self.DWH_ID.lower() + '_trg'
        self.TRG_DB_USERNAME = self.ADMIN_POSTGRES_USERNAME
        self.TRG_DB_PASSWORD = self.ADMIN_POSTGRES_PASSWORD

        self.DEFAULT_ROWS_GHSEET_TITLE = self.DWH_ID + ' - Default Rows'
        self.MDM_GHSEET_TITLE = self.DWH_ID + ' - Master Data Mappings'

    def setDwhId(self, dwhId):
        if dwhId != '' and dwhId is not None:
            self.DWH_ID = dwhId.upper()

    def setGoogleAPIKeyFilename(self, apiKeyFilename):
        if apiKeyFilename is None or apiKeyFilename == '':
            raise ValueError('No filename provided for the Google API ' +
                             'key. This is required for BETL to operate')
        self.GOOGLE_API_KEY_FILENAME = apiKeyFilename

    def setGoogleAccount(self, googleAccount):
        if googleAccount is None or googleAccount == '':
            raise ValueError('No Google account provided. ' +
                             'This is required for BETL to operate')
        self.GOOGLE_ACCOUNT = googleAccount

    def setAdminPostgresUsername(self, adminPostgresUsername):
        if adminPostgresUsername is None or adminPostgresUsername == '':
            raise ValueError('No admin username has been provided for the ' +
                             'Postgres database. This is required for BETL ' +
                             'to operate')
        self.ADMIN_POSTGRES_USERNAME = adminPostgresUsername

    def setAdminPostgresPassword(self, adminPostgresPassword):
        if adminPostgresPassword is not None and adminPostgresPassword != '':
            self.ADMIN_POSTGRES_PASSWORD = adminPostgresPassword

    def setAppRootPath(self, appRootPath):
        if appRootPath is not None and appRootPath != '':
            self.APP_ROOT_PATH = appRootPath

    def setTmpDataPath(self, tmpDataPath):
        if tmpDataPath is not None and tmpDataPath != '':
            self.TMP_DATA_PATH = self.APP_ROOT_PATH + '/' + tmpDataPath

    def setSrcDataPath(self, srcDataPath):
        if srcDataPath is not None and srcDataPath != '':
            self.SRC_DATA_PATH = self.APP_ROOT_PATH + '/' + srcDataPath

    def setReportsPath(self, reportsPath):
        if reportsPath is not None and reportsPath != '':
            self.REPORTS_PATH = self.APP_ROOT_PATH + '/' + reportsPath

    def setLogsPath(self, logsPath):
        if logsPath is not None and logsPath != '':
            self.LOG_PATH = self.APP_ROOT_PATH + '/' + logsPath

    def setSchemaPath(self, schemaPath):
        if schemaPath is not None and schemaPath != '':
            self.SCHEMA_PATH = self.APP_ROOT_PATH + '/' + schemaPath

    def setSchemaDescETLGsheetTitle(self, etlGSheetTitle):
        if etlGSheetTitle is None or etlGSheetTitle == '':
            self.SCHEMA_DESC_ETL_GSHEET_TITLE = self.DWH_ID + ' - ETL DB SCHEMA'
        else:
            self.SCHEMA_DESC_ETL_GSHEET_TITLE = etlGSheetTitle

    def setSchemaDescTRGGsheetTitle(self, trgGSheetTitle):
        if trgGSheetTitle is None or trgGSheetTitle == '':
            self.SCHEMA_DESC_TRG_GSHEET_TITLE = self.DWH_ID + ' - TRG DB SCHEMA'
        else:
            self.SCHEMA_DESC_TRG_GSHEET_TITLE = trgGSheetTitle

    def setETLDBHostName(self, etlDBHostName):
        if etlDBHostName is not None and etlDBHostName != '':
            self.ETL_DB_HOST_NAME = etlDBHostName

    def setETLDBName(self, etlDBName):
        if etlDBName is None or etlDBName == '':

            self.ETL_DB_NAME = self.DWH_ID.lower() + '_etl'
        else:
            self.ETL_DB_NAME = etlDBName

    def setETLDBUsername(self, etlDBUsername):
        if etlDBUsername is None or etlDBUsername == '':
            if self.ADMIN_POSTGRES_USERNAME is None:
                raise ValueError('Cannot assign a default value for the ' +
                                 'ETL DB username. Either specify one, or ' +
                                 'set the admin postgres username first')
            else:
                self.ETL_DB_USERNAME = self.ADMIN_POSTGRES_USERNAME
        else:
            self.ETL_DB_USERNAME = etlDBUsername

    def setETLDBPassword(self, etlDBPassword):
        if etlDBPassword is None or etlDBPassword == '':
            self.ETL_DB_PASSWORD = self.ADMIN_POSTGRES_PASSWORD
        else:
            self.ETL_DB_PASSWORD = etlDBPassword

    def setTRGDBHostName(self, trgDBHostName):
        if trgDBHostName is not None and trgDBHostName != '':
            self.TRG_DB_HOST_NAME = trgDBHostName

    def setTRGDBName(self, trgDBName):
        if trgDBName is None or trgDBName == '':
            self.TRG_DB_NAME = self.DWH_ID.lower() + '_trg'
        else:
            self.TRG_DB_NAME = trgDBName

    def setTRGDBUsername(self, trgDBUsername):
        if trgDBUsername is None or trgDBUsername == '':
            if self.ADMIN_POSTGRES_USERNAME is None:
                raise ValueError('Cannot assign a default value for the ' +
                                 'ETL DB username. Either specify one, or ' +
                                 'set the admin postgres username first')
            else:
                self.TRG_DB_USERNAME = self.ADMIN_POSTGRES_USERNAME
        else:
            self.TRG_DB_USERNAME = trgDBUsername

    def setTRGDBPassword(self, trgDBPassword):
        if trgDBPassword is None or trgDBPassword == '':
            self.TRG_DB_PASSWORD = self.ADMIN_POSTGRES_PASSWORD
        else:
            self.TRG_DB_PASSWORD = trgDBPassword

    def setDefaultRowsGSheetTitle(self, defaultRowsGSheetTitle):
        if defaultRowsGSheetTitle is None or defaultRowsGSheetTitle == '':
            self.DEFAULT_ROWS_GHSEET_TITLE = self.DWH_ID + ' - Default Rows'
        else:
            self.DEFAULT_ROWS_GHSEET_TITLE = defaultRowsGSheetTitle

    def setMDMGSheetTitle(self, mdmGSheetTitle):
        if mdmGSheetTitle is None or mdmGSheetTitle == '':
            self.MDM_GHSEET_TITLE = self.DWH_ID + ' - Master Data Mappings'
        else:
            self.MDM_GHSEET_TITLE = mdmGSheetTitle
