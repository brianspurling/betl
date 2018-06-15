import datetime
import pandas as pd
import os
from configobj import ConfigObj

from . import logger
from . import betlConfig

from .datastore import PostgresDatastore
from .datastore import SqliteDatastore
from .datastore import GsheetDatastore
from .datastore import ExcelDatastore
from .datastore import FileDatastore
from .dataLayer import SrcDataLayer
from .dataLayer import StgDataLayer
from .dataLayer import TrgDataLayer
from .dataLayer import SumDataLayer
from .ctrlDB import CtrlDB


#
# Conf() is a wrapper class to contain the "child" classes, making it easy
# to pass all the config around, whilst also keeping the code manageable
#
class Conf():

    def __init__(self, appConfigFile, runTimeParams, scheduleConfig):

        # We use the ConfigObj package to process our appConfigFile, which is
        # divided up into sections corresponding to the "child" classes below
        allConfig = ConfigObj(appConfigFile)

        # BETL's configuration is split into the following "child" classes
        self.ctrl = Ctrl(allConfig['ctrl'])
        self.exe = Exe(runTimeParams)
        self.state = State()
        self.schedule = Schedule(scheduleConfig)
        self.data = Data(
            config=allConfig['data'],
            includeDMDate=scheduleConfig['DEFAULT_DM_DATE'])


class Ctrl():

    def __init__(self, config):

        self.config = config

        self.DWH_ID = self.config['DWH_ID']
        self.TMP_DATA_PATH = self.config['TMP_DATA_PATH']
        self.LOG_PATH = self.config['LOG_PATH']

        self.CTRL_DB = CtrlDB(self.config['ctl_db'])

        # We can't initialise our JOB_LOG until we have an execID, which we
        # pull from the CTL DB, which means we need to create our Conf() object
        # first (and initialise logging and assign it to JOB_LOG later)
        self.JOB_LOG = None


class Data():

    def __init__(self, config, includeDMDate):

        self.config = config

        self.DATABASES = betlConfig.databases
        self.DATA_LAYERS = betlConfig.dataLayers
        self.AUDIT_COLS = pd.DataFrame(betlConfig.auditColumns)
        self.SRC_SYSTEM_LIST = []
        self.INCLUDE_DM_DATE = includeDMDate

        for srcSysID in self.config['src_sys']:
            self.SRC_SYSTEM_LIST.append(srcSysID)

        # The following are either datastore(s) or require datastores to
        # initialise them. Therefore we init these as-and-when we need them,
        # to avoid long delays at the start of every execution
        self.SCHEMA_DESCRIPTION_GSHEETS = {}
        self.LOGICAL_DATA_MODELS = {}
        self.DWH_DATABASES = {}
        self.DEFAULT_ROW_SRC = None
        self.MDM_SRC = None
        self.SRC_SYSTEMS = {}

        # We'll need these later, when we connect to the various Google Sheets
        self.apiUrl = self.config['schema_descs']['GSHEETS_API_URL']
        self.apiKey = self.config['schema_descs']['GSHEETS_API_KEY_FILE']

    def getSchemaDescGSheetDatastore(self, dbID):
        if dbID in self.SCHEMA_DESCRIPTION_GSHEETS:
            return self.SCHEMA_DESCRIPTION_GSHEETS[dbID]
        else:
            self.SCHEMA_DESCRIPTION_GSHEETS[dbID] = \
                GsheetDatastore(
                    ssID=dbID,
                    apiUrl=self.apiUrl,
                    apiKey=self.apiKey,
                    filename=self.config['schema_descs'][dbID + '_FILENAME'],
                    isSchemaDesc=True)
            return self.SCHEMA_DESCRIPTION_GSHEETS[dbID]

    def getLogicalDataModel(self, dataLayerID):
        if dataLayerID in self.LOGICAL_DATA_MODELS:
            return self.LOGICAL_DATA_MODELS[dataLayerID]
        else:
            if dataLayerID == 'SRC':
                self.LOGICAL_DATA_MODELS['SRC'] = SrcDataLayer(self)
            elif dataLayerID == 'STG':
                self.LOGICAL_DATA_MODELS['STG'] = StgDataLayer(self)
            elif dataLayerID == 'TRG':
                self.LOGICAL_DATA_MODELS['TRG'] = TrgDataLayer(self)
            elif dataLayerID == 'SUM':
                self.LOGICAL_DATA_MODELS['SUM'] = SumDataLayer(self)
            return self.LOGICAL_DATA_MODELS[dataLayerID]

    def getDWHDatastore(self, dbID):
        if dbID in self.DWH_DATABASES:
            return self.DWH_DATABASES[dbID]
        else:
            self.DWH_DATABASES[dbID] = \
                PostgresDatastore(
                    dbID=dbID,
                    host=self.config['dwh_dbs'][dbID]['HOST'],
                    dbName=self.config['dwh_dbs'][dbID]['DBNAME'],
                    user=self.config['dwh_dbs'][dbID]['USER'],
                    password=self.config['dwh_dbs'][dbID]['PASSWORD'],
                    createIfNotFound=True)
            return self.DWH_DATABASES[dbID]

    def getDefaultRowsDatastore(self):
        if self.DEFAULT_ROW_SRC is not None:
            return self.DEFAULT_ROW_SRC
        else:
            self.DEFAULT_ROW_SRC = \
                GsheetDatastore(
                    ssID='DR',
                    apiUrl=self.config['default_rows']['GSHEETS_API_URL'],
                    apiKey=self.config['default_rows']['GSHEETS_API_KEY_FILE'],
                    filename=self.config['default_rows']['FILENAME'])
            return self.DEFAULT_ROW_SRC

    def getMDMDatastore(self):
        if self.MDM_SRC is not None:
            return self.MDM_SRC
        else:
            self.MDM_SRC = \
                GsheetDatastore(
                    ssID='MDM',
                    apiUrl=self.config['mdm']['GSHEETS_API_URL'],
                    apiKey=self.config['mdm']['GSHEETS_API_KEY_FILE'],
                    filename=self.config['mdm']['FILENAME'])
            return self.MDM_SRC

    def getSrcSysDatastore(self, ssID):
        if ssID in self.SRC_SYSTEMS:
            return self.SRC_SYSTEMS[ssID]
        else:

            logger.logInitialiseSrcSysDatastore(
                datastoreID=ssID,
                datastoreType=self.config['src_sys'][ssID]['TYPE'])

            if self.config['src_sys'][ssID]['TYPE'] == 'POSTGRES':
                self.SRC_SYSTEMS[ssID] = \
                    PostgresDatastore(
                        dbID=ssID,
                        host=self.config['src_sys'][ssID]['HOST'],
                        dbName=self.config['src_sys'][ssID]['DBNAME'],
                        user=self.config['src_sys'][ssID]['USER'],
                        password=self.config['src_sys'][ssID]['PASSWORD'],
                        isSrcSys=True)

            elif self.config['src_sys'][ssID]['TYPE'] == 'SQLITE':
                self.SRC_SYSTEMS[ssID] = \
                    SqliteDatastore(
                        dbID=ssID,
                        path=self.config['src_sys'][ssID]['PATH'],
                        filename=self.config['src_sys'][ssID]['FILENAME'],
                        isSrcSys=True)

            elif self.config['src_sys'][ssID]['TYPE'] == 'FILESYSTEM':
                self.SRC_SYSTEMS[ssID] = \
                    FileDatastore(
                        fileSysID=ssID,
                        path=self.config['src_sys'][ssID]['PATH'],
                        fileExt=self.config['src_sys'][ssID]['FILE_EXT'],
                        delim=self.config['src_sys'][ssID]['DELIMITER'],
                        quotechar=self.config['src_sys'][ssID]['QUOTECHAR'],
                        isSrcSys=True)

            elif self.config['src_sys'][ssID]['TYPE'] == 'GSHEET':
                apiUrl = self.config['src_sys'][ssID]['GSHEETS_API_URL']
                apiKey = self.config['src_sys'][ssID]['GSHEETS_API_KEY_FILE']
                self.SRC_SYSTEMS[ssID] = \
                    GsheetDatastore(
                        ssID=ssID,
                        apiUrl=apiUrl,
                        apiKey=apiKey,
                        filename=self.config['src_sys'][ssID]['FILENAME'],
                        isSrcSys=True)

            elif self.config['src_sys'][ssID]['TYPE'] == 'EXCEL':
                self.SRC_SYSTEMS[ssID] = \
                    ExcelDatastore(
                        ssID=ssID,
                        path=self.config['src_sys'][ssID]['PATH'],
                        filename=self.config['src_sys'][ssID]['FILENAME'] +
                        self.config['src_sys'][ssID]['FILE_EXT'],
                        isSrcSys=True)

            return self.SRC_SYSTEMS[ssID]


class Exe():

    def __init__(self, params):
        self.LOG_LEVEL = params['LOG_LEVEL']

        self.SKIP_WARNINGS = params['SKIP_WARNINGS']

        self.BULK_OR_DELTA = params['BULK_OR_DELTA']

        self.RUN_SETUP = params['RUN_SETUP']
        self.READ_SRC = params['READ_SRC']

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

        if self.DATA_LIMIT_ROWS:
            self.MONITOR_MEMORY_USAGE = False
        else:
            self.MONITOR_MEMORY_USAGE = True


class State():

    def __init__(self):

        self.EXEC_ID = None
        self.RERUN_PREV_JOB = False

        # Global state for which stage (E,T,L,S) we're on
        self.STAGE = 'STAGE NOT SET'

        # Global state for which function of the execution's schedule we're in
        self.FUNCTION_ID = None

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

    def setFunctionId(self, functionId):
        self.FUNCTION_ID = functionId


class Schedule():

    def __init__(self, scheduleConfig):
        self.DEFAULT_EXTRACT = scheduleConfig['DEFAULT_EXTRACT']
        self.DEFAULT_TRANSFORM = scheduleConfig['DEFAULT_TRANSFORM']
        self.DEFAULT_LOAD = scheduleConfig['DEFAULT_LOAD']
        self.DEFAULT_SUMMARISE = scheduleConfig['DEFAULT_SUMMARISE']
        self.DEFAULT_DM_DATE = scheduleConfig['DEFAULT_DM_DATE']
        self.DEFAULT_DM_AUDIT = scheduleConfig['DEFAULT_DM_AUDIT']
        self.SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT = \
            scheduleConfig['SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT']
        self.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD = \
            scheduleConfig['TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD']
        self.EXTRACT_DFS = scheduleConfig['EXTRACT_DFS']
        self.TRANSFORM_DFS = scheduleConfig['TRANSFORM_DFS']
        self.LOAD_DFS = scheduleConfig['LOAD_DFS']
        self.SUMMARISE_DFS = scheduleConfig['SUMMARISE_DFS']
