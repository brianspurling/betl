import sys
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from .ctrlDB import CtrlDB
from . import df_dmDate


class Setup():

    def __init__(self):
        self.GOOGLE_API_SCOPE = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
        self.SCHEMAS_PATH = 'schemas'

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

    def setTmpDataPath(self, tmpDataPath):
        if tmpDataPath == '':
            self.TMP_DATA_PATH = 'tmp_data'
        else:
            self.TMP_DATA_PATH = tmpDataPath

    def setSrcDataPath(self, srcDataPath):
        if srcDataPath == '':
            self.SRC_DATA_PATH = 'src_data'
        else:
            self.SRC_DATA_PATH = srcDataPath

    def setReportsPath(self, reportsPath):
        if reportsPath == '':
            self.REPORTS_PATH = 'reports'
        else:
            self.REPORTS_PATH = reportsPath

    def setLogsPath(self, logsPath):
        if logsPath == '':
            self.LOG_PATH = 'logs'
        else:
            self.LOG_PATH = logsPath

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
            self.CTL_DB_NAME = ctlDBUsername

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

    # Create the directories and default files

    def createDirectories(self, response):
        if response.lower() in ['y', '']:
            if not os.path.exists(self.TMP_DATA_PATH):
                os.makedirs(self.TMP_DATA_PATH)
            if not os.path.exists(self.REPORTS_PATH):
                os.makedirs(self.REPORTS_PATH)
            if not os.path.exists(self.LOG_PATH):
                os.makedirs(self.LOG_PATH)
            if not os.path.exists(self.SRC_DATA_PATH):
                os.makedirs(self.SRC_DATA_PATH)
            if not os.path.exists(self.SCHEMAS_PATH):
                os.makedirs(self.SCHEMAS_PATH)

    def createGitignoreFile(self, response):
        if response.lower() in ['y', '']:
            op = ''
            op += '# BETL-specific' + '\n'
            op += self.TMP_DATA_PATH + '/' + '\n'
            op += self.REPORTS_PATH + '/' + '\n'
            op += self.LOG_PATH + '/' + '\n'
            op += 'schemas/' + '\n'
            op += self.GOOGLE_API_KEY_FILENAME + '\n'
            op += '\n'
            op += '# Jupyter Notebooks' + '\n'
            op += '.ipynb_checkpoints/' + '\n'
            op += '*.ipynb' + '\n'
            op += '\n'
            op += '# Byte-compiled / optimized / DLL files' + '\n'
            op += '.__pycache__/' + '\n'
            op += '*.py[cod]' + '\n'
            op += '*$py.class' + '\n'
            op += '\n'
            op += '# Distribution / packaging' + '\n'
            op += '.Python/' + '\n'
            op += 'eggs/' + '\n'
            op += '.eggs/' + '\n'
            op += '*.egg-info/' + '\n'
            op += '*.egg' + '\n'

            with open('.gitignore', 'w+') as f:
                f.write(op)

    def createAppConfigFile(self, response):
        if response.lower() in ['y', '']:
            op = ''
            op += "[ctrl] \n"
            op += "\n"
            op += "  DWH_ID = " + self.DWH_ID.lower() + " \n"
            op += "  TMP_DATA_PATH = " + self.TMP_DATA_PATH + " \n"
            op += "  REPORTS_PATH = " + self.REPORTS_PATH + " \n"
            op += "  LOG_PATH = " + self.LOG_PATH + " \n"
            op += "\n"
            op += "  [[ctl_db]] \n"
            op += " \n"
            op += "      HOST = " + self.CTL_DB_HOST_NAME + " \n"
            op += "      DBNAME = " + self.CTL_DB_NAME + " \n"
            op += "      USER = " + self.CTL_DB_USERNAME + " \n"
            op += "      PASSWORD = " + self.CTL_DB_PASSWORD + " \n"
            op += "\n"
            op += "[data] \n"
            op += "\n"
            op += "  GSHEETS_API_KEY_FILE = " + self.GOOGLE_API_KEY_FILENAME
            op += " \n"
            op += " \n"
            op += "  [[schema_descs]] \n"
            op += " \n"
            op += "      ETL_FILENAME = " + self.SCHEMA_DESC_ETL_GSHEET_TITLE
            op += " \n"
            op += "      TRG_FILENAME = " + self.SCHEMA_DESC_TRG_GSHEET_TITLE
            op += " \n"
            op += " \n"
            op += "  [[dwh_dbs]] \n"
            op += " \n"
            op += "    [[[ETL]]] \n"
            op += "      HOST = " + self.ETL_DB_HOST_NAME + " \n"
            op += "      DBNAME = " + self.ETL_DB_NAME + " \n"
            op += "      USER = " + self.ETL_DB_USERNAME + " \n"
            op += "      PASSWORD = " + self.ETL_DB_PASSWORD + " \n"
            op += " \n"
            op += "    [[[TRG]]] \n"
            op += "      HOST = " + self.TRG_DB_HOST_NAME + " \n"
            op += "      DBNAME = " + self.TRG_DB_NAME + " \n"
            op += "      USER = " + self.TRG_DB_USERNAME + " \n"
            op += "      PASSWORD = " + self.TRG_DB_PASSWORD + " \n"
            op += " \n"
            op += "  [[default_rows]] \n"
            op += " \n"
            op += "    FILENAME = " + self.DEFAULT_ROWS_GHSEET_TITLE + " \n"
            op += " \n"
            op += "  [[mdm]] \n"
            op += " \n"
            op += "    FILENAME = " + self.MDM_GHSEET_TITLE + " \n"
            op += " \n"
            op += "  [[src_sys]] \n"
            op += " \n"
            op += "    [[[SQLITE_EXAMPLE]]] \n"
            op += "      TYPE = SQLITE \n"
            op += "      PATH = src_data/ \n"
            op += "      FILENAME = \n"
            op += " \n"
            op += "    [[[FILESYSTEM_EXAMPLE]]] \n"
            op += "      TYPE = FILESYSTEM \n"
            op += "      PATH = src_data/ \n"
            op += "      FILE_EXT = .csv \n"
            op += "      DELIMITER = ',' \n"
            op += "      QUOTECHAR = '\"' \n"
            op += " \n"
            op += "    [[[GSHEET_EXAMPLE]]] \n"
            op += "      TYPE = GSHEET \n"
            op += "      FILENAME = \n"
            op += " \n"
            op += "    [[[EXCEL_EXAMPLE]]] \n"
            op += "      TYPE = EXCEL \n"
            op += "      PATH = src_data/ \n"
            op += "      FILENAME =  \n"
            op += "      FILE_EXT = .xlsx \n"

            with open('appConfig.ini', 'w+') as f:
                f.write(op)

    def createMainScript(self, response):
        if response.lower() in ['y', '']:
            op = ''
            op += "from betl import Betl\n"
            op += "import sys\n"
            op += "\n"
            op += "scheduleConfig = {\n"
            op += "\n"
            op += "    # Control whether default transformations should be run"
            op += "\n"
            op += "    'DEFAULT_EXTRACT': True,\n"
            op += "    'DEFAULT_TRANSFORM': True,\n"
            op += "    'DEFAULT_LOAD': True,\n"
            op += "    'DEFAULT_SUMMARISE': True,\n"
            op += "\n"
            op += "    # Data BETL can generate itself\n"
            op += "    'DEFAULT_DM_DATE': True,\n"
            op += "    'DEFAULT_DM_AUDIT': True,\n"
            op += "\n"
            op += "    # Define tables to exclude from default processing\n"
            op += "    'SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],\n"
            op += "    'TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],\n"
            op += "\n"
            op += "    # Here you define the bespoke parts of your data "
            op += "pipeline.\n"
            op += "    'EXTRACT_DATAFLOWS': [\n"
            op += "        df_example.exampleDataflow]\n"
            op += "    'TRANSFORM_DATAFLOWS': [\n"
            op += "    ],\n"
            op += "    'LOAD_DATAFLOWS': [],\n"
            op += "    'SUMMARISE_DATAFLOWS': []\n"
            op += "}\n"
            op += "\n"
            op += "betl = Betl(appConfigFile='./appConfig.ini',\n"
            op += "            scheduleConfig=scheduleConfig,\n"
            op += "            runTimeParams=sys.argv)\n"
            op += "\n"
            op += "betl.run()\n"

            with open('main.py', 'w+') as f:
                f.write(op)

    def createExampleDataflow(self, response):
        if response.lower() in ['y', '']:
            op = ""
            op += "def exampleDataflow(betl):\n"
            op += "\n"
            op += "    dfl = betl.DataFlow(desc='Example dataflow')\n"
            op += "\n"
            op += "    dfl.read(\n"
            op += "        tableName='src_example_table_name',\n"
            op += "        dataLayer='SRC')\n"
            op += "\n"
            op += "    dfl.dedupe(\n"
            op += "        dataset='src_example_table_name',\n"
            op += "        desc='Make dataset unique')\n"
            op += "\n"
            op += "    dfl.write(\n"
            op += "        dataset='src_example_table_name',\n"
            op += "        targetTableName='trg_dm_example',\n"
            op += "        dataLayerID='STG')\n"
            with open('df_example.py', 'w+') as f:
                f.write(op)

    # Create the database and GSheets

    def createDatabases(self, response):
        if response.lower() in ['y', '']:
            con = psycopg2.connect(
                dbname='postgres',
                user=self.CTL_DB_USERNAME,
                host=self.CTL_DB_HOST_NAME,
                password=self.CTL_DB_PASSWORD)

            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            cur = con.cursor()
            cur.execute("DROP DATABASE IF EXISTS " + self.CTL_DB_NAME + ";")
            cur.execute("CREATE DATABASE " + self.CTL_DB_NAME + ";")

            ctrlDB = CtrlDB(
                host=self.CTL_DB_HOST_NAME,
                dbName=self.CTL_DB_NAME,
                username=self.CTL_DB_USERNAME,
                password=self.CTL_DB_PASSWORD)
            ctrlDB.createExecutionsTable()
            ctrlDB.createFunctionsTable()
            ctrlDB.createDataflowsTable()
            ctrlDB.createStepsTable()

            print('Control DB: ' + self.CTL_DB_NAME)

            con = psycopg2.connect(
                dbname='postgres',
                user=self.ETL_DB_USERNAME,
                host=self.ETL_DB_HOST_NAME,
                password=self.ETL_DB_PASSWORD)

            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            cur = con.cursor()
            cur.execute("DROP DATABASE IF EXISTS " + self.ETL_DB_NAME + ";")
            cur.execute("CREATE DATABASE " + self.ETL_DB_NAME + ";")

            print('ETL DB: ' + self.ETL_DB_NAME)

            con = psycopg2.connect(
                dbname='postgres',
                user=self.TRG_DB_USERNAME,
                host=self.TRG_DB_HOST_NAME,
                password=self.TRG_DB_PASSWORD)

            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            cur = con.cursor()
            cur.execute("DROP DATABASE IF EXISTS " + self.TRG_DB_NAME + ";")
            cur.execute("CREATE DATABASE " + self.TRG_DB_NAME + ";")

            print('TRG DB: ' + self.TRG_DB_NAME)

    def createSchemaDescGSheets(self, response):
        # TODO: error if titles already exist? and below.
        if response.lower() in ['y', '']:

            etl = self.GSPREAD.create(self.SCHEMA_DESC_ETL_GSHEET_TITLE)
            etl.share(
                self.GOOGLE_ACCOUNT,
                perm_type='user',
                role='writer')

            trg = self.GSPREAD.create(self.SCHEMA_DESC_TRG_GSHEET_TITLE)
            trg.share(
                self.GOOGLE_ACCOUNT,
                perm_type='user',
                role='writer')

            # print('ETL Schema Description Google Sheet: ' + etl.)
            # print('TRG Schema Description Google Sheet: ' + trg.)

    def createMDMGsheet(self, response):
        if response.lower() in ['y', '']:
            mdm = self.GSPREAD.create(self.MDM_GHSEET_TITLE)
            mdm.share(
                self.GOOGLE_ACCOUNT,
                perm_type='user',
                role='writer')

    def createDefaultRowsGsheet(self, response):
        if response.lower() in ['y', '']:
            dr = self.GSPREAD.create(self.DEFAULT_ROWS_GHSEET_TITLE)
            data = df_dmDate.getDefaultRows()
            colHeadings = list(data[0].keys())
            ws = dr.get_worksheet(0)

            cells = ws.range(1, 1, len(data)+1, len(data[0]))

            count = 0
            for colHeading in colHeadings:
                cells[count].value = colHeading
                count += 1

            for row in data:
                for colName in row:
                    cells[count].value = row[colName]
                    count += 1

            ws.update_cells(cells)
            ws.update_title('dm_date')
            dr.share(
                self.GOOGLE_ACCOUNT,
                perm_type='user',
                role='writer')
