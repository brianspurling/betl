import os


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
        if not os.path.exists(self.SCHEMA_PATH):
            os.makedirs(self.SCHEMA_PATH)


def createGitignoreFile(self, response):
    if response.lower() in ['y', '']:
        op = ''
        op += '# BETL-specific' + '\n'
        op += self.TMP_DATA_PATH + '/' + '\n'
        op += self.REPORTS_PATH + '/' + '\n'
        op += self.LOG_PATH + '/' + '\n'
        op += 'schema/' + '\n'
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

        with open(self.APP_ROOT_PATH + '/' + '.gitignore', 'w+') as f:
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
        op += "  SCHEMA_PATH = " + self.SCHEMA_PATH + " \n"
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

        with open(self.APP_ROOT_PATH + '/' + 'appConfig.ini', 'w+') as f:
            f.write(op)


def createMainScript(self, response):
    if response.lower() in ['y', '']:
        op = ''
        op += "import betl\n"
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
        op += "        dfl_example.exampleDataflow]\n"
        op += "    'TRANSFORM_DATAFLOWS': [\n"
        op += "    ],\n"
        op += "    'LOAD_DATAFLOWS': [],\n"
        op += "    'SUMMARISE_DATAFLOWS': []\n"
        op += "}\n"
        op += "\n"
        op += "pl = betl.pipeline(appConfigFile='./appConfig.ini',\n"
        op += "                   scheduleConfig=scheduleConfig,\n"
        op += "                   runTimeParams=sys.argv)\n"
        op += "\n"
        op += "pl.run()\n"

        with open(self.APP_ROOT_PATH + '/' + 'main.py', 'w+') as f:
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
        with open(self.APP_ROOT_PATH + '/' + 'dfl_example.py', 'w+') as f:
            f.write(op)
