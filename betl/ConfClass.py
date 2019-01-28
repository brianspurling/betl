import datetime
import pandas as pd
import airflow
import os

from betl.io import PostgresDatastore
from betl.io import SqliteDatastore
from betl.io import GsheetDatastore
from betl.io import ExcelDatastore
from betl.io import FileDatastore
from betl.datamodel import DataLayer
from betl.logger import Logger

from betl.dataflow import DataFlow

AIRFLOW_DEFAULT_ARGS = {
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
        'LOAD_DIM_DATAFLOWS': [],
        'LOAD_FACT_DATAFLOWS': [],
        'SUMMARISE_DATAFLOWS': []
    }

    def __init__(self, conf):

        appDirectory = conf['appDirectory']
        appConfig = conf['appConfig']
        scheduleConfig = conf['scheduleConfig']
        self.IS_ADMIN = conf['isAdmin']
        self.IS_AIRFLOW = conf['isAirflow']

        self.APP_DIRECTORY = appDirectory

        #########################
        # DATASTORE CONNECTIONS #
        #########################

        # These will all eventually hold connections to various data sources
        # (databases, spreadsheets, file systems, etc). But we don't want to
        # set these up on init, because this will make it slooooow. Instead,
        # there are class methods below that get-or-create the connections
        # as-and-when they're needed.

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

        # Schema description Gsheet
        self.SCHEMA_DESCRIPTION_GSHEETS_FILENAMES = {}
        for dbId in appConfig['data']['schema_descs']:
            self.SCHEMA_DESCRIPTION_GSHEETS_FILENAMES[dbId] = \
                appConfig['data']['schema_descs'][dbId]

        # Default rows Gsheet
        if 'default_rows' not in appConfig['data']:
            # You do not have to specify a default_rows source in appConfig
            # if you don't want to use default rows in your dimensions
            self.DEFAULT_ROWS_FILENAME = None
        else:
            self.DEFAULT_ROWS_FILENAME = \
                appConfig['data']['default_rows']['FILENAME']

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

        ####################
        # GSHEET API DEETS #
        ####################

        self.GOOGLE_API_SCOPE = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

        self.GSHEETS_API_KEY_FILE = appConfig['data']['GSHEETS_API_KEY_FILE']

        ######################
        # SCHEDULE AND MODEL #
        ######################

        self.BULK_OR_DELTA = appConfig['ctrl']['BULK_OR_DELTA'].upper()

        self.RUN_EXTRACT = appConfig['ctrl']['RUN_EXTRACT'].upper()
        self.RUN_TRANSFORM = appConfig['ctrl']['RUN_TRANSFORM'].upper()
        self.RUN_LOAD = appConfig['ctrl']['RUN_LOAD'].upper()
        self.RUN_DM_LOAD = appConfig['ctrl']['RUN_DM_LOAD'].upper()
        self.RUN_FT_LOAD = appConfig['ctrl']['RUN_FT_LOAD'].upper()
        self.RUN_SUMMARISE = appConfig['ctrl']['RUN_SUMMARISE'].upper()
        self.RUN_DATAFLOWS = appConfig['ctrl']['RUN_DATAFLOWS'].upper()

        self.DEFAULT_DM_DATE = appConfig['ctrl']['DEFAULT_DM_DATE'].upper()
        self.DEFAULT_DM_AUDIT = appConfig['ctrl']['DEFAULT_DM_AUDIT'].upper()

        # betlAdmin will set up a conf without a schedule
        if scheduleConfig is not None:
            self.DEFAULT_EXTRACT = scheduleConfig['DEFAULT_EXTRACT']
            self.DEFAULT_LOAD = scheduleConfig['DEFAULT_LOAD']
            self.DEFAULT_SUMMARISE = scheduleConfig['DEFAULT_SUMMARISE']
            self.EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT = \
                scheduleConfig['EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT']
            self.BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD = \
                scheduleConfig['BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD']
            self.EXTRACT_DATAFLOWS = scheduleConfig['EXTRACT_DATAFLOWS']
            self.TRANSFORM_DATAFLOWS = scheduleConfig['TRANSFORM_DATAFLOWS']
            self.LOAD_DIM_DATAFLOWS = scheduleConfig['LOAD_DIM_DATAFLOWS']
            self.LOAD_FACT_DATAFLOWS = scheduleConfig['LOAD_FACT_DATAFLOWS']
            self.SUMMARISE_DATAFLOWS = scheduleConfig['SUMMARISE_DATAFLOWS']

        ###########
        # CONTROL #
        ###########

        self.DWH_ID = appConfig['ctrl']['DWH_ID']
        self.LOG_LEVEL = appConfig['ctrl']['LOG_LEVEL'].upper()
        self.SKIP_WARNINGS = appConfig['ctrl']['SKIP_WARNINGS']
        self.WRITE_TO_ETL_DB = appConfig['ctrl']['WRITE_TO_ETL_DB']
        self.DATA_LIMIT_ROWS = int(appConfig['ctrl']['DATA_LIMIT_ROWS'])
        self.RUN_TESTS = appConfig['ctrl']['RUN_TESTS']

        if self.DATA_LIMIT_ROWS:
            self.MONITOR_MEMORY_USAGE = False
        else:
            self.MONITOR_MEMORY_USAGE = True
        self.AUDIT_COLS = pd.DataFrame(Conf.auditColumns)

        #######################
        # DIRECTORY LOCATIONS #
        #######################

        self.TMP_DATA_PATH = \
            self.APP_DIRECTORY + appConfig['ctrl']['TMP_DATA_PATH']
        self.REPORTS_PATH = \
            self.APP_DIRECTORY + appConfig['ctrl']['REPORTS_PATH']
        self.LOG_PATH = \
            self.APP_DIRECTORY + appConfig['ctrl']['LOG_PATH']
        self.SCHEMA_PATH = \
            self.APP_DIRECTORY + appConfig['ctrl']['SCHEMA_PATH']

        #########
        # STATE #
        #########

        self.LAST_EXEC_REPORT = None
        self.EXE_START_TIME = datetime.datetime.now()

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

        ##########
        # LOGGER #
        ##########

        # We don't init this now for Airflow executions, otherwise every
        # poll for DAGs will create a new log file! Instead, it is set by
        # the operator wrapper funtcion at the start of each task execution
        self.LOG = None
        if not self.IS_AIRFLOW:
            self.LOG = Logger(self)

    #######################
    # LOGICAL DWH SCHEMAS #
    #######################

    # We build the logical DWH schemas outside of init, so that conf
    # can be loaded by the admin module before reading/refreshing the
    # schema descs from file
    def constructLogicalDWHSchemas(self):
        # An object oriented representation of our BETL databases (ETL, TRG)
        self.DWH_LOGICAL_SCHEMAS = {}
        for dlId in self.dataLayers:
            self.DWH_LOGICAL_SCHEMAS[dlId] = DataLayer(
                conf=self,
                dataLayerID=dlId)

    def log(self, logMethod, **kwargs):
        getattr(self.LOG, logMethod)(**kwargs)

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
                    conf=self,
                    dataLayerID=dataLayerID)
            return self.DWH_LOGICAL_SCHEMAS[dataLayerID]

    #########################
    # DATASTORE CONNECTIONS #
    #########################

    def getSchemaDescDatastore(self, dbId):
        if dbId in self.SCHEMA_DESCRIPTION_GSHEETS:
            return self.SCHEMA_DESCRIPTION_GSHEETS[dbId]
        else:
            self.log(
                'logInitialiseDatastore',
                datastoreID=dbId,
                datastoreType='GSHEET',
                isSchemaDesc=True)
            self.SCHEMA_DESCRIPTION_GSHEETS[dbId] = \
                GsheetDatastore(
                    ssID=dbId,
                    apiScope=self.GOOGLE_API_SCOPE,
                    apiKey=self.APP_DIRECTORY + self.GSHEETS_API_KEY_FILE,
                    filename=self.SCHEMA_DESCRIPTION_GSHEETS_FILENAMES[dbId],
                    isSchemaDesc=True)
            return self.SCHEMA_DESCRIPTION_GSHEETS[dbId]

    def getDefaultRowsDatastore(self):
        if self.DEFAULT_ROWS_GSHEET is not None:
            return self.DEFAULT_ROWS_GSHEET
        elif self.DEFAULT_ROWS_FILENAME is not None:
            self.log(
                'logInitialiseDatastore',
                datastoreID='DR',
                datastoreType='GSHEET')
            self.DEFAULT_ROWS_GSHEET = \
                GsheetDatastore(
                    ssID='DR',
                    apiScope=self.GOOGLE_API_SCOPE,
                    apiKey=self.APP_DIRECTORY + self.GSHEETS_API_KEY_FILE,
                    filename=self.DEFAULT_ROWS_FILENAME)
            return self.DEFAULT_ROWS_GSHEET
        else:
            # You don't have to specify a default rows file at all
            return None

    def getMDMDatastore(self):
        if self.MDM_GSHEET is not None:
            return self.MDM_GSHEET
        else:
            self.log(
                'logInitialiseDatastore',
                datastoreID='MDM',
                datastoreType='GSHEET')
            self.MDM_GSHEET = \
                GsheetDatastore(
                    ssID='MDM',
                    apiScope=self.GOOGLE_API_SCOPE,
                    apiKey=self.APP_DIRECTORY + self.GSHEETS_API_KEY_FILE,
                    filename=self.MDM_FILENAME)
            return self.MDM_SRC

    def getDWHDatastore(self, dbId):

        if self.DWH_DATABASES is None:
            self.DWH_DATABASES = {}

        if dbId in self.DWH_DATABASES:
            return self.DWH_DATABASES[dbId]
        else:
            self.log(
                'logInitialiseDatastore',
                datastoreID=dbId,
                datastoreType='POSTGRES')
            self.DWH_DATABASES[dbId] = \
                PostgresDatastore(
                    dbId=dbId,
                    host=self.DWH_DATABASES_DETAILS[dbId]['host'],
                    dbName=self.DWH_DATABASES_DETAILS[dbId]['dbName'],
                    user=self.DWH_DATABASES_DETAILS[dbId]['user'],
                    password=self.DWH_DATABASES_DETAILS[dbId]['password'],
                    createIfNotFound=True)
            return self.DWH_DATABASES[dbId]

    def getSrcSysDatastore(self, ssID):
        if ssID in self.SRC_SYSTEMS:
            return self.SRC_SYSTEMS[ssID]
        else:
            srcSysType = self.SRC_SYSTEM_DETAILS[ssID]['type']
            if srcSysType in ['POSTGRES', 'SQLITE', 'FILESYSTEM', 'EXEL']:
                srcSysPath = (self.APP_DIRECTORY +
                              self.SRC_SYSTEM_DETAILS[ssID]['path'])
            if srcSysType == 'POSTGRES':
                self.SRC_SYSTEMS[ssID] = \
                    PostgresDatastore(
                        dbId=ssID,
                        host=self.SRC_SYSTEM_DETAILS[ssID]['host'],
                        dbName=self.SRC_SYSTEM_DETAILS[ssID]['dbname'],
                        user=self.SRC_SYSTEM_DETAILS[ssID]['user'],
                        password=self.SRC_SYSTEM_DETAILS[ssID]['PASSWORD'],
                        isSrcSys=True)

            elif srcSysType == 'SQLITE':

                self.SRC_SYSTEMS[ssID] = \
                    SqliteDatastore(
                        dbId=ssID,
                        path=srcSysPath,
                        filename=self.SRC_SYSTEM_DETAILS[ssID]['filename'],
                        isSrcSys=True)

            elif srcSysType == 'FILESYSTEM':
                self.SRC_SYSTEMS[ssID] = \
                    FileDatastore(
                        fileSysID=ssID,
                        path=srcSysPath,
                        fileExt=self.SRC_SYSTEM_DETAILS[ssID]['file_ext'],
                        delim=self.SRC_SYSTEM_DETAILS[ssID]['delimiter'],
                        quotechar=self.SRC_SYSTEM_DETAILS[ssID]['quotechar'],
                        isSrcSys=True)

            elif srcSysType == 'EXCEL':
                self.SRC_SYSTEMS[ssID] = \
                    ExcelDatastore(
                        ssID=ssID,
                        path=srcSysPath,
                        filename=self.SRC_SYSTEM_DETAILS[ssID]['filename'] +
                        self.SRC_SYSTEM_DETAILS[ssID]['file_ext'],
                        isSrcSys=True)

            elif srcSysType == 'GSHEET':
                self.SRC_SYSTEMS[ssID] = \
                    GsheetDatastore(
                        ssID=ssID,
                        apiScope=self.GOOGLE_API_SCOPE,
                        apiKey=self.APP_DIRECTORY + self.GSHEETS_API_KEY_FILE,
                        filename=self.SRC_SYSTEM_DETAILS[ssID]['filename'],
                        isSrcSys=True)

            return self.SRC_SYSTEMS[ssID]

    # For easier manual access, temp data files are prefixed with an
    # incrementing number when they are saved to disk. So to easily access
    # files during execution, we maintain a file name mapping:
    # filename the code expects --> actual filename on disk (ie incl prefix)
    def populateTempDataFileNameMap(self):

        for root, directories, filenames in os.walk(self.TMP_DATA_PATH):
            for filename in filenames:
                _filename = filename[self.FILE_PREFIX_LENGTH+1:]
                shortfn, ext = os.path.splitext(filename)
                if ext == '.csv':
                    thisPrefix = int(filename[:self.FILE_PREFIX_LENGTH])
                    if thisPrefix >= self.NEXT_FILE_PREFIX:
                        self.NEXT_FILE_PREFIX = thisPrefix + 1
                    self.FILE_NAME_MAP[_filename] = filename

        self.log('logPopulateTempDataFileNameMapEnd')

    ##########################
    # DATAFLOW CREATE METHOD #
    ##########################

    # We have this helper function here so application code
    # can more easily create dataflows (because the conf object
    # is being passed into every airflow operator anyway)
    def DataFlow(self, desc):
        return DataFlow(desc=desc, conf=self)
