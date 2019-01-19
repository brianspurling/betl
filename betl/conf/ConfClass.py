import datetime
import pandas as pd
import airflow

from betl.io import PostgresDatastore
from betl.io import SqliteDatastore
from betl.io import GsheetDatastore
from betl.io import ExcelDatastore
from betl.io import FileDatastore
from betl.datamodel import DataLayer

airflowArgs = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)  # TODO: what should this be?
}

class Conf():

    # Hardcoded configuration for the BETL framework

    dwhDatabases = ['ETL', 'TRG']

    dataLayers = {'EXT': 'ETL',
                  'TRN': 'ETL',
                  'LOD': 'ETL',
                  'BSE': 'TRG',
                  'SUM': 'TRG'}

    auditColumns = {
        'colNames': [
            'audit_source_system',
            'audit_bulk_load_date',
            'audit_latest_delta_load_date',
            'audit_latest_load_operation'
        ],
        'dataType': [
            'TEXT',
            'DATE',
            'DATE',
            'TEXT'
        ],
    }

    # Defaults for configurations intended to be passed in by the app

    defaultScheduleConfig = {
        'DEFAULT_EXTRACT': False,
        'EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],
        'DEFAULT_LOAD': False,
        'DEFAULT_SUMMARISE': False,
        'DEFAULT_DM_DATE': False,
        'BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],
        'EXTRACT_DATAFLOWS': [],
        'TRANSFORM_DATAFLOWS': [],
        'LOAD_DATAFLOWS': [],
        'SUMMARISE_DATAFLOWS': []
    }


    def __init__(self, betl, appConfig, scheduleConfig):

        self.BETL = betl

        #########################
        # DATASTORE CONNECTIONS #
        #########################

        # These will all eventually hold connections to various data sources
        # (databases, spreadsheets, file systems, etc). But we don't want to
        # set these up on init, because this will make it slooooow. Instead,
        # there are class methods below that get-or-create the connections
        # as-and-when they're needed

        # Control database
        self.CTRL_DB = None

        # GSheets
        self.SCHEMA_DESCRIPTION_GSHEETS = {}
        self.DEFAULT_ROWS_GSHEET = None
        self.MDM_GSHEET = None

        # Pipeline Databases (i.e. ETL and TRG)
        self.DWH_DATABASES = {}

        # Source system datastores (databases/spreadsheets/etc)
        self.SRC_SYSTEMS = {}

        ################################
        # DATASTORE CONNECTION DETAILS #
        ################################

        # Ctrl DB

        self.CTRL_DB_DETAILS = {
            'host': appConfig['ctrl']['ctl_db']['HOST'],
            'dbName': appConfig['ctrl']['ctl_db']['DBNAME'],
            'user': appConfig['ctrl']['ctl_db']['USER'],
            'password': appConfig['ctrl']['ctl_db']['PASSWORD']}

        # Schema description Gsheet
        self.SCHEMA_DESCRIPTION_GSHEETS_FILENAMES = {}
        for dbId in appConfig['data']['schema_descs']:
            self.SCHEMA_DESCRIPTION_GSHEETS_FILENAMES[dbId] = appConfig['data']['schema_descs'][dbId]

        # Default rows Gsheet
        if 'default_rows' not in appConfig['data']:
            # You do not have to specify a default_rows source in appConfig
            # if you don't want to use default rows in your dimensions
            self.DEFAULT_ROWS_FILENAME = None
        else:
            self.DEFAULT_ROWS_FILENAME = appConfig['data']['default_rows']['FILENAME']

        # MDM Ghseet
        self.MDM_FILENAME = appConfig['data']['mdm']['FILENAME']

        # Data warehouse databases
        self.DWH_DATABASES_DETAILS = {}
        for dbId in Conf.dwhDatabases:
            self.DWH_DATABASES_DETAILS[dbId] = {
                'host': appConfig['data']['dwh_dbs'][dbId]['HOST'],
                'dbName': appConfig['data']['dwh_dbs'][dbId]['DBNAME'],
                'user': appConfig['data']['dwh_dbs'][dbId]['USER'],
                'password': appConfig['data']['dwh_dbs'][dbId]['PASSWORD']}

        # Source system datastores
        # Different datastores will have different details
        self.SRC_SYSTEM_DETAILS = {}
        for srcSysID in appConfig['data']['src_sys']:
            self.SRC_SYSTEM_DETAILS[srcSysID] = {}
            for attr in appConfig['data']['src_sys'][srcSysID]:
                self.SRC_SYSTEM_DETAILS[srcSysID][attr.lower()] = \
                    appConfig['data']['src_sys'][srcSysID][attr]

        ###############
        # DATA MODELS #
        ###############

        # TODO rename to schemas
        # An object oriented representation of our BETL databases (ETL, TRG)
        self.DWH_LOGICAL_SCHEMAS = {}
        # A nested dictionary representation of our source systems datastores
        self.SRC_SYSTEM_DATA_MODELS = {}

        ####################
        # GSHEET API DEETS #
        ####################

        self.GOOGLE_API_SCOPE = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

        self.GSHEETS_API_KEY_FILE = appConfig['data']['GSHEETS_API_KEY_FILE']

        ############
        # SCHEDULE #
        ############

        self.BULK_OR_DELTA = appConfig['ctrl']['BULK_OR_DELTA']

        self.RUN_EXTRACT = appConfig['ctrl']['RUN_EXTRACT']
        self.RUN_TRANSFORM = appConfig['ctrl']['RUN_TRANSFORM']
        self.RUN_LOAD = appConfig['ctrl']['RUN_LOAD']
        self.RUN_DM_LOAD = appConfig['ctrl']['RUN_DM_LOAD']
        self.RUN_FT_LOAD = appConfig['ctrl']['RUN_FT_LOAD']
        self.RUN_SUMMARISE = appConfig['ctrl']['RUN_SUMMARISE']
        self.RUN_DATAFLOWS = appConfig['ctrl']['RUN_DATAFLOWS']

        self.DEFAULT_EXTRACT = scheduleConfig['DEFAULT_EXTRACT']
        self.DEFAULT_LOAD = scheduleConfig['DEFAULT_LOAD']
        self.DEFAULT_SUMMARISE = scheduleConfig['DEFAULT_SUMMARISE']
        self.DEFAULT_DM_DATE = scheduleConfig['DEFAULT_DM_DATE']
        self.DEFAULT_DM_AUDIT = scheduleConfig['DEFAULT_DM_AUDIT']
        self.EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT = \
            scheduleConfig['EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT']
        self.BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD = \
            scheduleConfig['BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD']
        self.EXTRACT_DATAFLOWS = scheduleConfig['EXTRACT_DATAFLOWS']
        self.TRANSFORM_DATAFLOWS = scheduleConfig['TRANSFORM_DATAFLOWS']
        self.LOAD_DATAFLOWS = scheduleConfig['LOAD_DATAFLOWS']
        self.SUMMARISE_DATAFLOWS = scheduleConfig['SUMMARISE_DATAFLOWS']

        ###########
        # CONTROL #
        ###########

        self.DWH_ID = appConfig['ctrl']['DWH_ID']
        self.LOG_LEVEL = appConfig['ctrl']['LOG_LEVEL'].upper()
        self.SKIP_WARNINGS = appConfig['ctrl']['SKIP_WARNINGS']
        self.WRITE_TO_ETL_DB = appConfig['ctrl']['WRITE_TO_ETL_DB']
        self.DATA_LIMIT_ROWS = appConfig['ctrl']['DATA_LIMIT_ROWS']
        self.RUN_TESTS = appConfig['ctrl']['RUN_TESTS']

        if self.DATA_LIMIT_ROWS:
            self.MONITOR_MEMORY_USAGE = False
        else:
            self.MONITOR_MEMORY_USAGE = True
        self.AUDIT_COLS = pd.DataFrame(Conf.auditColumns)

        #######################
        # DIRECTORY LOCATIONS #
        #######################

        self.TMP_DATA_PATH = appConfig['ctrl']['TMP_DATA_PATH']
        self.REPORTS_PATH = appConfig['ctrl']['REPORTS_PATH']
        self.LOG_PATH = appConfig['ctrl']['LOG_PATH']
        self.SCHEMA_PATH = appConfig['ctrl']['SCHEMA_PATH']

        #########
        # STATE #
        #########

        self.LAST_EXEC_REPORT = None
        self.EXEC_ID = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.RERUN_PREV_JOB = False

        # Global state for which stage (E,T,L,S) we're on, and for which
        # function of the execution's schedule we're in
        self.STAGE = 'STAGE NOT SET'
        self.FUNCTION_ID = None

        def setStage(self, stage):
            self.STAGE = stage

        def setFunctionId(self, functionId):
            self.FUNCTION_ID = functionId

        # These dictate the start/end dates of dm_date. They can be overridden
        # at any point in the application's ETL process, providing the
        # generateDMDate function is added to the schedule _after_ the
        # functions in which they're set
        self.EARLIEST_DATE_IN_DATA = datetime.date(1900, 1, 1)
        self.LATEST_DATE_IN_DATA = (datetime.date.today() +
                                    datetime.timedelta(days=365))

        # The mapping controls for the temp data file names (because we prefix
        # them with an incrementing number to make manual access easier)
        self.FILE_NAME_MAP = {}
        self.NEXT_FILE_PREFIX = 1
        self.FILE_PREFIX_LENGTH = 4

    ###############
    # DATA MODELS #
    ###############

    # The DataModel is an object oriented representation of a physical schema
    # of a database. It is comprised of multiple DataLayers, each containing
    # 1+ DataSets, each containing 1+ Tables, each containing 1+ columns.
    # DataLayer is the top Class in the hierarchy, so although we're
    # retrieving an entire "data model" we reference it by the data layer's ID
    def getLogicalSchemaDataLayer(self, dataLayerID):
        if dataLayerID in self.DWH_LOGICAL_SCHEMAS:
            return self.DWH_LOGICAL_SCHEMAS[dataLayerID]
        else:
            self.DWH_LOGICAL_SCHEMAS[dataLayerID] = \
                DataLayer(
                    betl=self.BETL,
                    dataLayerID=dataLayerID)
            return self.DWH_LOGICAL_SCHEMAS[dataLayerID]

    #########################
    # DATASTORE CONNECTIONS #
    #########################

    def getCtrlDatastore(self):
        if self.CTRL_DB is not None:
            return self.CTRL_DB
        else:
            # TODO: we can't log this datastore, because we connect to
            # it before logging is initialised
            self.CTRL_DB = PostgresDatastore(
                                dbId='CTL',
                                host=self.CTRL_DB_DETAILS['host'],
                                dbName=self.CTRL_DB_DETAILS['dbName'],
                                user=self.CTRL_DB_DETAILS['user'],
                                password=self.CTRL_DB_DETAILS['password'],
                                createIfNotFound=True)
            return self.CTRL_DB

    def getSchemaDescDatastore(self, dbId):
        if dbId in self.SCHEMA_DESCRIPTION_GSHEETS:
            return self.SCHEMA_DESCRIPTION_GSHEETS[dbId]
        else:
            self.BETL.LOG.logInitialiseDatastore(dbId, 'GSHEET', isSchemaDesc=True)
            self.SCHEMA_DESCRIPTION_GSHEETS[dbId] = \
                GsheetDatastore(
                    ssID=dbId,
                    apiScope=self.GOOGLE_API_SCOPE,
                    apiKey=self.GSHEETS_API_KEY_FILE,
                    filename=self.SCHEMA_DESCRIPTION_GSHEETS_FILENAMES[dbId],
                    isSchemaDesc=True)
            return self.SCHEMA_DESCRIPTION_GSHEETS[dbId]

    def getDefaultRowsDatastore(self):
        if self.DEFAULT_ROWS_GSHEET is not None:
            return self.DEFAULT_ROWS_GSHEET
        elif self.DEFAULT_ROWS_FILENAME is not None:
            self.BETL.LOG.logInitialiseDatastore('DR', 'GSHEET')
            self.DEFAULT_ROWS_GSHEET = \
                GsheetDatastore(
                    ssID='DR',
                    apiScope=self.GOOGLE_API_SCOPE,
                    apiKey=self.GSHEETS_API_KEY_FILE,
                    filename=self.DEFAULT_ROWS_FILENAME)
            return self.DEFAULT_ROWS_GSHEET
        else:
            # You don't have to specify a default rows file at all
            return None

    def getMDMDatastore(self):
        if self.MDM_GSHEET is not None:
            return self.MDM_GSHEET
        else:
            self.BETL.LOG.logInitialiseDatastore('MDM', 'GSHEET')
            self.MDM_GSHEET = \
                GsheetDatastore(
                    ssID='MDM',
                    apiScope=self.GOOGLE_API_SCOPE,
                    apiKey=self.GSHEETS_API_KEY_FILE,
                    filename=self.MDM_FILENAME)
            return self.MDM_SRC

    def getDWHDatastore(self, dbId):

        if self.DWH_DATABASES is None:
            self.DWH_DATABASES = {}

        if dbId in self.DWH_DATABASES:
            return self.DWH_DATABASES[dbId]
        else:
            self.BETL.LOG.logInitialiseDatastore(dbId, 'POSTGRES')
            self.DWH_DATABASES[dbId] = \
                PostgresDatastore(
                    dbId=dbId,
                    host=self.DWH_DATABASES_DETAILS[dbId]['HOST'],
                    dbName=self.DWH_DATABASES_DETAILS[dbId]['DBNAME'],
                    user=self.DWH_DATABASES_DETAILS[dbId]['USER'],
                    password=self.DWH_DATABASES_DETAILS[dbId]['PASSWORD'],
                    createIfNotFound=True)
            return self.DWH_DATABASES[dbId]

    def getSrcSysDatastore(self, ssID):
        if ssID in self.SRC_SYSTEMS:
            return self.SRC_SYSTEMS[ssID]
        else:
            if self.SRC_SYSTEM_DETAILS[ssID]['type'] == 'POSTGRES':
                self.SRC_SYSTEMS[ssID] = \
                    PostgresDatastore(
                        dbId=ssID,
                        host=self.SRC_SYSTEM_DETAILS[ssID]['host'],
                        dbName=self.SRC_SYSTEM_DETAILS[ssID]['dbname'],
                        user=self.SRC_SYSTEM_DETAILS[ssID]['user'],
                        password=self.SRC_SYSTEM_DETAILS[ssID]['PASSWORD'],
                        isSrcSys=True)

            elif self.SRC_SYSTEM_DETAILS[ssID]['type'] == 'SQLITE':
                self.SRC_SYSTEMS[ssID] = \
                    SqliteDatastore(
                        dbId=ssID,
                        path=self.SRC_SYSTEM_DETAILS[ssID]['path'],
                        filename=self.SRC_SYSTEM_DETAILS[ssID]['filename'],
                        isSrcSys=True)

            elif self.SRC_SYSTEM_DETAILS[ssID]['type'] == 'FILESYSTEM':
                self.SRC_SYSTEMS[ssID] = \
                    FileDatastore(
                        fileSysID=ssID,
                        path=self.SRC_SYSTEM_DETAILS[ssID]['path'],
                        fileExt=self.SRC_SYSTEM_DETAILS[ssID]['file_ext'],
                        delim=self.SRC_SYSTEM_DETAILS[ssID]['delimiter'],
                        quotechar=self.SRC_SYSTEM_DETAILS[ssID]['quotechar'],
                        isSrcSys=True)

            elif self.SRC_SYSTEM_DETAILS[ssID]['type'] == 'GSHEET':
                self.SRC_SYSTEMS[ssID] = \
                    GsheetDatastore(
                        ssID=ssID,
                        apiScope=self.GOOGLE_API_SCOPE,
                        apiKey=self.GSHEETS_API_KEY_FILE,
                        filename=self.SRC_SYSTEM_DETAILS[ssID]['filename'],
                        isSrcSys=True)

            elif self.SRC_SYSTEM_DETAILS[ssID]['type'] == 'EXCEL':
                self.SRC_SYSTEMS[ssID] = \
                    ExcelDatastore(
                        ssID=ssID,
                        path=self.SRC_SYSTEM_DETAILS[ssID]['path'],
                        filename=self.SRC_SYSTEM_DETAILS[ssID]['filename'] +
                        self.SRC_SYSTEM_DETAILS[ssID]['file_ext'],
                        isSrcSys=True)

            return self.SRC_SYSTEMS[ssID]
