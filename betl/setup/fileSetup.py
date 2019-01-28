import os
import shutil
import tempfile
import datetime
import time
import json
import ast


def archiveLogFiles(conf):

    timestamp = datetime.datetime.fromtimestamp(
        time.time()
    ).strftime('%Y%m%d%H%M%S')

    source = conf.LOG_PATH
    dest = conf.LOG_PATH + '/archive_' + timestamp + '/'

    if not os.path.exists(dest):
        os.makedirs(dest)

    files = os.listdir(source)
    for file in files:
        if file.find('jobLog') > -1:
            shutil.move(source + '/' + file, dest)
        if file.find('alerts') > -1:
            shutil.move(source + '/' + file, dest)


def createReportsDir(conf):
    if (os.path.exists(conf.REPORTS_PATH)):
        tmp = tempfile.mktemp(dir=os.path.dirname(conf.REPORTS_PATH))
        shutil.move(conf.REPORTS_PATH, tmp)  # rename
        shutil.rmtree(tmp)  # delete
    os.makedirs(conf.REPORTS_PATH)  # create the new folder


def createSchemaDir(conf):
    # The srcTableName mapping file is created on auto-pop, i.e. once,
    # so we need to preserve it
    srcTableNameMappingFile = conf.SCHEMA_PATH + '/srcTableNameMapping.txt'
    tableNameMap = None
    try:
        mapFile = open(srcTableNameMappingFile, 'r')
        tableNameMap = ast.literal_eval(mapFile.read())
    except FileNotFoundError:
        pass

    if os.path.exists(conf.SCHEMA_PATH + '/'):
        shutil.rmtree(conf.SCHEMA_PATH + '/')
    os.makedirs(conf.SCHEMA_PATH + '/')
    open(conf.SCHEMA_PATH + '/lastModifiedTimes.txt', 'a').close()

    if tableNameMap is not None:
        with open(srcTableNameMappingFile, 'w+') as file:
            file.write(json.dumps(tableNameMap))


def deleteTemporaryData(conf):

    path = conf.TMP_DATA_PATH.replace('/', '')

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

    conf.log('logDeleteTemporaryDataEnd')


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
        op += "    'DEFAULT_LOAD': True,\n"
        op += "    'DEFAULT_SUMMARISE': True,\n"
        op += "\n"
        op += "    # Data BETL can generate itself\n"
        op += "    'DEFAULT_DM_DATE': True,\n"
        op += "    'DEFAULT_DM_AUDIT': True,\n"
        op += "\n"
        op += "    # Define tables to exclude from default processing\n"
        op += "    'EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],\n"
        op += "    'BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],\n"
        op += "\n"
        op += "    # Here you define the bespoke parts of your data "
        op += "pipeline.\n"
        op += "    'EXTRACT_DATAFLOWS': [\n"
        op += "        dfl_example.exampleDataflow]\n"
        op += "    'TRANSFORM_DATAFLOWS': [\n"
        op += "    ],\n"
        op += "    'LOAD_DIM_DATAFLOWS': [],\n"
        op += "    'LOAD_FACT_DATAFLOWS': [],\n"
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
        op += "        tableName='example_table_name',\n"
        op += "        dataLayer='EXT')\n"
        op += "\n"
        op += "    dfl.dedupe(\n"
        op += "        dataset='example_table_name',\n"
        op += "        desc='Make dataset unique')\n"
        op += "\n"
        op += "    dfl.prepForLoad(\n"
        op += "        dataset='example_table_name',\n"
        op += "        targetTableName='dm_example')\n"
        with open(self.APP_ROOT_PATH + '/' + 'dfl_example.py', 'w+') as f:
            f.write(op)
