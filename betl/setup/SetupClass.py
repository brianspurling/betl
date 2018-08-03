import sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials


class Setup():

    # To keep the code maintainable, we have divied up the class' functions
    # across multiple modules.  So we must import these all into the class.
    # This module, with the Setup() class, contains all the basic "set"
    # functions, where info is input into the class.

    from .fileSetup import (createDirectories,
                            createGitignoreFile,
                            createAppConfigFile,
                            createMainScript,
                            createExampleDataflow)

    from .dbSetup import (createDatabases)

    from .gsheetSetup import (createSchemaDescGSheets,
                              createMDMGsheet,
                              createDefaultRowsGsheet)

    def __init__(self):
        self.GOOGLE_API_SCOPE = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    def setDwhId(self, dwhId):
        if dwhId == '':
            self.DWH_ID = 'DWH'
        else:
            self.DWH_ID = dwhId.upper()

    def setGoogleAPIKeyFilename(self, apiKeyFilename):
        if apiKeyFilename == '':
            apiKeyFilename = input('You must provide a filename! >> ')
            if apiKeyFilename == '':
                sys.exit()
        self.GOOGLE_API_KEY_FILENAME = apiKeyFilename
        self.GSPREAD = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                self.GOOGLE_API_KEY_FILENAME,
                self.GOOGLE_API_SCOPE))

    def setGoogleAccount(self, googleAccount):
        if googleAccount == '':
            googleAccount = input('You must provide a Google account! >> ')
            if googleAccount == '':
                sys.exit()
        self.GOOGLE_ACCOUNT = googleAccount

    def setAdminPostgresUsername(self, adminPostgresUsername):
        if adminPostgresUsername == '':
            adminPostgresUsername = \
                input('You must provide an admin username! >> ')
            if adminPostgresUsername == '':
                sys.exit()
        self.ADMIN_POSTGRES_USERNAME = adminPostgresUsername

    def setAdminPostgresPassword(self, adminPostgresPassword):
        if adminPostgresPassword == '':
            self.ADMIN_POSTGRES_PASSWORD = ""
        else:
            self.ADMIN_POSTGRES_PASSWORD = adminPostgresPassword

    def setAppRootPath(self, appRootPath):
        if appRootPath == '':
            self.APP_ROOT_PATH = '.'
        else:
            self.APP_ROOT_PATH = appRootPath

    def setTmpDataPath(self, tmpDataPath):
        self.TMP_DATA_PATH = self.APP_ROOT_PATH + '/'
        if tmpDataPath == '':
            self.TMP_DATA_PATH += 'tmp_data'
        else:
            self.TMP_DATA_PATH += tmpDataPath

    def setSrcDataPath(self, srcDataPath):
        self.SRC_DATA_PATH = self.APP_ROOT_PATH + '/'
        if srcDataPath == '':
            self.SRC_DATA_PATH += 'src_data'
        else:
            self.SRC_DATA_PATH += srcDataPath

    def setReportsPath(self, reportsPath):
        self.REPORTS_PATH = self.APP_ROOT_PATH + '/'
        if reportsPath == '':
            self.REPORTS_PATH += 'reports'
        else:
            self.REPORTS_PATH += reportsPath

    def setLogsPath(self, logsPath):
        self.LOG_PATH = self.APP_ROOT_PATH + '/'
        if logsPath == '':
            self.LOG_PATH += 'logs'
        else:
            self.LOG_PATH += logsPath

    def setSchemaPath(self, schemaPath):
        self.SCHEMA_PATH = self.APP_ROOT_PATH + '/'
        if schemaPath == '':
            self.SCHEMA_PATH += 'schema'
        else:
            self.SCHEMA_PATH += schemaPath

    def setCtlDBHostName(self, ctlDBHostName):
        if ctlDBHostName == '':
            self.CTL_DB_HOST_NAME = 'localhost'
        else:
            self.CTL_DB_HOST_NAME = ctlDBHostName

    def setCtlDBName(self, ctlDBName):
        if ctlDBName == '':
            self.CTL_DB_NAME = self.DWH_ID.lower() + '_ctl'
        else:
            self.CTL_DB_NAME = ctlDBName

    def setCtlDBUsername(self, ctlDBUsername):
        if ctlDBUsername == '':
            self.CTL_DB_USERNAME = self.ADMIN_POSTGRES_USERNAME
        else:
            self.CTL_DB_USERNAME = ctlDBUsername

    def setCtlDBPassword(self, ctlDBPassword):
        if ctlDBPassword == '':
            self.CTL_DB_PASSWORD = self.ADMIN_POSTGRES_PASSWORD
        else:
            self.CTL_DB_PASSWORD = ctlDBPassword

    def setSchemaDescETLGsheetTitle(self, etlGSheetTitle):
        if etlGSheetTitle == '':
            self.SCHEMA_DESC_ETL_GSHEET_TITLE = \
                self.DWH_ID + ' - ETL DB SCHEMA'
        else:
            self.SCHEMA_DESC_ETL_GSHEET_TITLE = etlGSheetTitle

    def setSchemaDescTRGGsheetTitle(self, trgGSheetTitle):
        if trgGSheetTitle == '':
            self.SCHEMA_DESC_TRG_GSHEET_TITLE = \
                self.DWH_ID + ' - TRG DB SCHEMA'
        else:
            self.SCHEMA_DESC_TRG_GSHEET_TITLE = trgGSheetTitle

    def setETLDBHostName(self, etlDBHostName):
        if etlDBHostName == '':
            self.ETL_DB_HOST_NAME = 'localhost'
        else:
            self.ETL_DB_HOST_NAME = etlDBHostName

    def setETLDBName(self, etlDBName):
        if etlDBName == '':
            self.ETL_DB_NAME = self.DWH_ID.lower() + '_etl'
        else:
            self.ETL_DB_NAME = etlDBName

    def setETLDBUsername(self, etlDBUsername):
        if etlDBUsername == '':
            self.ETL_DB_USERNAME = self.ADMIN_POSTGRES_USERNAME
        else:
            self.ETL_DB_USERNAME = etlDBUsername

    def setETLDBPassword(self, etlDBPassword):
        if etlDBPassword == '':
            self.ETL_DB_PASSWORD = self.ADMIN_POSTGRES_PASSWORD
        else:
            self.ETL_DB_PASSWORD = etlDBPassword

    def setTRGDBHostName(self, trgDBHostName):
        if trgDBHostName == '':
            self.TRG_DB_HOST_NAME = 'localhost'
        else:
            self.TRG_DB_HOST_NAME = trgDBHostName

    def setTRGDBName(self, trgDBName):
        if trgDBName == '':
            self.TRG_DB_NAME = self.DWH_ID.lower() + '_trg'
        else:
            self.TRG_DB_NAME = trgDBName

    def setTRGDBUsername(self, trgDBUsername):
        if trgDBUsername == '':
            self.TRG_DB_USERNAME = self.ADMIN_POSTGRES_USERNAME
        else:
            self.TRG_DB_USERNAME = trgDBUsername

    def setTRGDBPassword(self, trgDBPassword):
        if trgDBPassword == '':
            self.TRG_DB_PASSWORD = self.ADMIN_POSTGRES_PASSWORD
        else:
            self.TRG_DB_PASSWORD = trgDBPassword

    def setDefaultRowsGSheetTitle(self, defaultRowsGSheetTitle):
        if defaultRowsGSheetTitle == '':
            self.DEFAULT_ROWS_GHSEET_TITLE = self.DWH_ID + ' - Default Rows'
        else:
            self.DEFAULT_ROWS_GHSEET_TITLE = defaultRowsGSheetTitle

    def setMDMGSheetTitle(self, mdmGSheetTitle):
        if mdmGSheetTitle == '':
            self.MDM_GHSEET_TITLE = self.DWH_ID + ' - Master Data Mappings'
        else:
            self.MDM_GHSEET_TITLE = mdmGSheetTitle
