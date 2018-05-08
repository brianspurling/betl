import datetime
import pandas as pd
import os
from configobj import ConfigObj

from . import logger
from .datastore import PostgresDatastore
from .datastore import SqliteDatastore
from .datastore import SpreadsheetDatastore
from .datastore import FileDatastore
from .dataLayer import SrcDataLayer
from .dataLayer import StgDataLayer
from .dataLayer import TrgDataLayer
from .dataLayer import SumDataLayer
from .ctrlDB import CtrlDB


#
# The ExeConf object holds our runtime configuration
# and (for the time being) our global state
#
class Conf():

    def __init__(self, appConfigFile, runTimeParams, scheduleConfig):

        self.configObj = ConfigObj(appConfigFile)

        self.ctrl = Ctrl(self.configObj)
        self.exe = Exe(runTimeParams)
        self.state = State()
        self.schedule = Schedule(scheduleConfig)
        self.data = Data(self.configObj)

        self.MONITOR_MEMORY_USAGE = True
        self.LOGICAL_DATA_MODELS = {}

        self.JOB_LOG = None

        auditColumns_data = {
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
        self.auditColumns = pd.DataFrame(auditColumns_data)

        self.DATA_LAYERS = ['SRC', 'STG', 'TRG', 'SUM']

    # We need the conf to init the ctrl db, and we need the ctrl db to
    # get the exec Id, and we need the exec ID to init the logging
    def initialiseLogging(self):
        self.JOB_LOG = logger.initialiseLogging(self)

    def getLogicalDataModel(self, dataLayerID):
        if dataLayerID not in self.LOGICAL_DATA_MODELS:
            self.LOGICAL_DATA_MODELS[dataLayerID] = \
                self.buildLogicalDataModel(dataLayerID)
        return self.LOGICAL_DATA_MODELS[dataLayerID]

    def buildLogicalDataModel(self, dataLayerID):
        if dataLayerID == 'SRC':
            self.LOGICAL_DATA_MODELS['SRC'] = SrcDataLayer(self)
        elif dataLayerID == 'STG':
            self.LOGICAL_DATA_MODELS['STG'] = StgDataLayer(self)
        elif dataLayerID == 'TRG':
            self.LOGICAL_DATA_MODELS['TRG'] = TrgDataLayer(self)
        elif dataLayerID == 'SUM':
            self.LOGICAL_DATA_MODELS['SUM'] = SumDataLayer(self)
        return self.LOGICAL_DATA_MODELS[dataLayerID]


class Ctrl():

    def __init__(self, configObj):

        self.configObj = configObj
        self.LOG_PATH = configObj['LOG_PATH']
        self.CTRL_DB = CtrlDB(configObj['ctldb'])
        self.DWH_ID = configObj['DWH_ID']
        self.TMP_DATA_PATH = configObj['TMP_DATA_PATH']
        self.SCHEMA_DESCRIPTION_GSHEETS = None
        self.apiUrl = \
            configObj['schema_descriptions']['GOOGLE_SHEETS_API_URL']
        self.apiKey = \
            configObj['schema_descriptions']['GOOGLE_SHEETS_API_KEY_FILE']

    def getSchemaDescGSheetDatastores(self):
        if self.SCHEMA_DESCRIPTION_GSHEETS is None:
            self.SCHEMA_DESCRIPTION_GSHEETS = {}
            self.SCHEMA_DESCRIPTION_GSHEETS['ETL'] = \
                SpreadsheetDatastore(
                    ssID='ETL',
                    apiUrl=self.apiUrl,
                    apiKey=self.apiKey,
                    filename=self.configObj['schema_descriptions']
                    ['ETL_FILENAME'],
                    isSchemaDesc=True)
            self.SCHEMA_DESCRIPTION_GSHEETS['TRG'] = \
                SpreadsheetDatastore(
                    ssID='TRG',
                    apiUrl=self.apiUrl,
                    apiKey=self.apiKey,
                    filename=self.configObj['schema_descriptions']
                    ['TRG_FILENAME'],
                    isSchemaDesc=True)

        return self.SCHEMA_DESCRIPTION_GSHEETS


class Data():

    def __init__(self, configObj):
        self.configObj = configObj

        # We init these as-and-when we need them, to avoid
        # long delays at the start of very execution
        self.SRC_SYSTEMS = {}
        self.DEFAULT_ROW_SRC = None
        self.DWH_DATABASES = {}

    def getDatastore(self, dbID):
        if dbID not in self.DWH_DATABASES:
            dbConfigObj = self.configObj['dbs'][dbID]
            self.DWH_DATABASES[dbID] = \
                PostgresDatastore(
                    dbID=dbID,
                    host=dbConfigObj['HOST'],
                    dbName=dbConfigObj['DBNAME'],
                    user=dbConfigObj['USER'],
                    password=dbConfigObj['PASSWORD'],
                    createIfNotFound=True)

        return self.DWH_DATABASES[dbID]

    def getDefaultRowsDatastore(self):
        if self.DEFAULT_ROW_SRC is None:

            self.DEFAULT_ROW_SRC = \
                SpreadsheetDatastore(
                    ssID='DR',
                    apiUrl=self.configObj['default_rows']
                    ['GOOGLE_SHEETS_API_URL'],
                    apiKey=self.configObj['default_rows']
                    ['GOOGLE_SHEETS_API_KEY_FILE'],
                    filename=self.configObj['default_rows']['FILENAME'])
        return self.DEFAULT_ROW_SRC

    def getSrcSysDatastore(self, srcSysID):
        if srcSysID not in self.SRC_SYSTEMS:
            srcSysConfigObj = self.configObj['src_sys'][srcSysID]

            logger.logInitialiseSrcSysDatastore(
                datastoreID=srcSysID,
                datastoreType=srcSysConfigObj['TYPE'])

            if srcSysConfigObj['TYPE'] == 'POSTGRES':
                self.SRC_SYSTEMS[srcSysID] = \
                    PostgresDatastore(
                        dbID=srcSysID,
                        host=srcSysConfigObj['HOST'],
                        dbName=srcSysConfigObj['DBNAME'],
                        user=srcSysConfigObj['USER'],
                        password=srcSysConfigObj['PASSWORD'],
                        isSrcSys=True)

            elif srcSysConfigObj['TYPE'] == 'SQLITE':
                self.SRC_SYSTEMS[srcSysID] = \
                    SqliteDatastore(
                        dbID=srcSysID,
                        path=srcSysConfigObj['PATH'],
                        filename=srcSysConfigObj['FILENAME'],
                        isSrcSys=True)

            elif srcSysConfigObj['TYPE'] == 'FILESYSTEM':
                self.SRC_SYSTEMS[srcSysID] = \
                    FileDatastore(
                        fileSysID=srcSysID,
                        path=srcSysConfigObj['PATH'],
                        fileExt=srcSysConfigObj['FILE_EXT'],
                        delim=srcSysConfigObj['DELIMITER'],
                        quotechar=srcSysConfigObj['QUOTECHAR'],
                        isSrcSys=True)

            elif srcSysConfigObj['TYPE'] == 'SPREADSHEET':
                apiUrl = srcSysConfigObj['GOOGLE_SHEETS_API_URL']
                apiKey = srcSysConfigObj['GOOGLE_SHEETS_API_KEY_FILE']
                self.SRC_SYSTEMS[srcSysID] = \
                    SpreadsheetDatastore(
                        ssID=srcSysID,
                        apiUrl=apiUrl,
                        apiKey=apiKey,
                        filename=srcSysConfigObj['FILENAME'],
                        isSrcSys=True)

        return self.SRC_SYSTEMS[srcSysID]


class Exe():

    def __init__(self, params):
        self.LOG_LEVEL = params['LOG_LEVEL']

        self.SKIP_WARNINGS = params['SKIP_WARNINGS']

        self.BULK_OR_DELTA = params['BULK_OR_DELTA']

        self.RUN_SETUP = params['RUN_SETUP']

        self.RUN_REBUILD_ALL = params['RUN_REBUILD_ALL']
        self.RUN_REBUILD_SRC = params['RUN_REBUILD_SRC']
        self.RUN_REBUILD_STG = params['RUN_REBUILD_STG']
        self.RUN_REBUILD_TRG = params['RUN_REBUILD_TRG']
        self.RUN_REBUILD_SUM = params['RUN_REBUILD_SUM']

        self.RUN_EXTRACT = params['RUN_EXTRACT']
        self.RUN_TRANSFORM = params['RUN_TRANSFORM']
        self.RUN_LOAD = params['RUN_LOAD']
        self.RUN_DM_LOAD = params['RUN_DM_LOAD']
        self.RUN_FT_LOAD = params['RUN_FT_LOAD']
        self.RUN_SUMMARISE = params['RUN_SUMMARISE']

        self.DELETE_TMP_DATA = params['DELETE_TMP_DATA']

        self.RUN_DATAFLOWS = params['RUN_DATAFLOWS']

        self.WRITE_TO_ETL_DB = params['WRITE_TO_ETL_DB']

        self.DATA_LIMIT_ROWS = params['DATA_LIMIT_ROWS']


class State():

    def __init__(self):
        self.EXEC_ID = None
        self.RERUN_PREV_JOB = False
        # Global state for which stage (E,T,L) we're on
        self.STAGE = 'STAGE NOT SET'

        # These dictate the start/end dates of dm_date. They can be overridden
        # at any point in the application's ETL process, providing the
        # generateDMDate function is added to the schedule _after_ the
        # functions in which they're set
        self.EARLIEST_DATE_IN_DATA = datetime.date(1900, 1, 1)
        self.LATEST_DATE_IN_DATA = (datetime.date.today() +
                                    datetime.timedelta(days=365))

        self.fileNameMap = {}
        self.nextFilePrefix = 1
        self.filePrefixLength = 4

    def populateFileNameMap(self, tmpDataPath):
        for root, directories, filenames in os.walk(tmpDataPath):
            for filename in filenames:
                _filename = filename[self.filePrefixLength+1:]
                shortfn, ext = os.path.splitext(filename)
                if ext == '.csv':
                    thisPrefix = int(filename[:self.filePrefixLength])
                    if thisPrefix >= self.nextFilePrefix:
                        self.nextFilePrefix = thisPrefix + 1
                    self.fileNameMap[_filename] = filename

    def setStage(self, stage):
        self.STAGE = stage

    def setExecID(self, execID):
        self.EXEC_ID = execID


class Schedule():

    def __init__(self, scheduleConfig):
        self.DEFAULT_EXTRACT = scheduleConfig['DEFAULT_EXTRACT']
        self.DEFAULT_TRANSFORM = scheduleConfig['DEFAULT_TRANSFORM']
        self.DEFAULT_LOAD = scheduleConfig['DEFAULT_LOAD']
        self.DEFAULT_SUMMARISE = scheduleConfig['DEFAULT_SUMMARISE']
        self.DEFAULT_DM_DATE = scheduleConfig['DEFAULT_DM_DATE']
        self.SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT = \
            scheduleConfig['SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT']
        self.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD = \
            scheduleConfig['TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD']
        self.EXTRACT_DFS = scheduleConfig['EXTRACT_DFS']
        self.TRANSFORM_DFS = scheduleConfig['TRANSFORM_DFS']
        self.LOAD_DFS = scheduleConfig['LOAD_DFS']
        self.SUMMARISE_DFS = scheduleConfig['SUMMARISE_DFS']
