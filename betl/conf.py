import datetime
from configobj import ConfigObj
from .datastore import PostgresDatastore
from .datastore import SpreadsheetDatastore
from .datastore import FileDatastore


#
# The ExeConf object holds our runtime configuration
# and (for the time being) our global state
#
class Conf():

    def __init__(self, appConfigFile, runTimeParams, scheduleConfig):

        self.app = App(ConfigObj(appConfigFile))
        self.exe = Exe(runTimeParams)
        self.state = State()
        self.schedule = Schedule(scheduleConfig)


class App():

    def __init__(self, configObj):

        self.DWH_ID = configObj['DWH_ID']
        self.TMP_DATA_PATH = configObj['TMP_DATA_PATH']
        self.LOG_PATH = configObj['LOG_PATH']

        apiUrl = configObj['schema_descriptions']['GOOGLE_SHEETS_API_URL']
        apiKey = configObj['schema_descriptions']['GOOGLE_SHEETS_API_KEY_FILE']

        self.SCHEMA_DESCRIPTION_GSHEETS = {}
        self.SCHEMA_DESCRIPTION_GSHEETS['ETL'] = \
            SpreadsheetDatastore(
                ssID='ETL',
                apiUrl=apiUrl,
                apiKey=apiKey,
                filename=configObj['schema_descriptions']['ETL_FILENAME'])
        self.SCHEMA_DESCRIPTION_GSHEETS['TRG'] = \
            SpreadsheetDatastore(
                ssID='TRG',
                apiUrl=apiUrl,
                apiKey=apiKey,
                filename=configObj['schema_descriptions']['TRG_FILENAME'])

        self.DWH_DATABASES = {}
        for dbID in configObj['dbs']:
            dbConfigObj = configObj['dbs'][dbID]
            self.DWH_DATABASES[dbID] = \
                PostgresDatastore(
                    dbID=dbID,
                    host=dbConfigObj['HOST'],
                    dbName=dbConfigObj['DBNAME'],
                    user=dbConfigObj['USER'],
                    password=dbConfigObj['PASSWORD'])

        self.DEFAULT_ROW_SRC = \
            SpreadsheetDatastore(
                ssID='DR',
                apiUrl=configObj['default_rows']['GOOGLE_SHEETS_API_URL'],
                apiKey=configObj['default_rows']['GOOGLE_SHEETS_API_KEY_FILE'],
                filename=configObj['default_rows']['FILENAME'])

        self.SRC_SYSTEMS = {}
        for srcSysID in configObj['src_sys']:

            cnfgObj = configObj['src_sys'][srcSysID]

            if cnfgObj['TYPE'] == 'POSTGRES':
                self.SRC_SYSTEMS[srcSysID] = \
                    PostgresDatastore(
                        dbID=srcSysID,
                        host=cnfgObj['HOST'],
                        dbName=cnfgObj['DBNAME'],
                        user=cnfgObj['USER'],
                        password=cnfgObj['PASSWORD'])

            elif cnfgObj['TYPE'] == 'FILESYSTEM':
                self.SRC_SYSTEMS[srcSysID] = \
                    FileDatastore(
                        fileSysID=srcSysID,
                        path=cnfgObj['PATH'],
                        fileExt=cnfgObj['FILE_EXT'],
                        delim=cnfgObj['DELIMITER'],
                        quotechar=cnfgObj['QUOTECHAR'])

            elif cnfgObj['TYPE'] == 'SPREADSHEET':
                apiUrl = cnfgObj['GOOGLE_SHEETS_API_URL']
                apiKey = cnfgObj['GOOGLE_SHEETS_API_KEY_FILE']
                self.SRC_SYSTEMS[srcSysID] = \
                    SpreadsheetDatastore(
                        ssID=srcSysID,
                        apiUrl=apiUrl,
                        apiKey=apiKey,
                        filename=cnfgObj['FILENAME'])


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

        self.DELETE_TMP_DATA = params['DELETE_TMP_DATA']

        self.RUN_DATAFLOWS = params['RUN_DATAFLOWS']

        self.WRITE_TO_ETL_DB = params['WRITE_TO_ETL_DB']


class State():

    def __init__(self):
        self.EXEC_ID = None
        self.RERUN_PREV_JOB = False
        self.STAGE = None  # Global state for which stage (E,T,L) we're on

        # These dictate the start/end dates of dm_date. They can be overridden
        # at any point in the application's ETL process, providing the
        # generateDMDate function is added to the schedule _after_ the
        # functions in which they're set
        self.EARLIEST_DATE_IN_DATA = datetime.date(1900, 1, 1)
        self.LATEST_DATE_IN_DATA = (datetime.date.today() +
                                    datetime.timedelta(days=365))

        self.LOGICAL_DATA_MODELS = None

    def setStage(self, stage):
        self.STAGE = stage

    def setExecID(self, execID):
        self.EXEC_ID = execID

    def setLogicalDataModels(self, logicalDataModels):
        self.LOGICAL_DATA_MODELS = logicalDataModels


class Schedule():

    def __init__(self, scheduleConfig):
        self.DEFAULT_EXTRACT = scheduleConfig['DEFAULT_EXTRACT']
        self.SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT = \
            scheduleConfig['SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT']
        self.DEFAULT_LOAD = scheduleConfig['DEFAULT_LOAD']
        self.DEFAULT_DM_DATE = scheduleConfig['DEFAULT_DM_DATE']
        self.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD = \
            scheduleConfig['TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD']
        self.EXTRACT_DFS = scheduleConfig['EXTRACT_DFS']
        self.TRANSFORM_DFS = scheduleConfig['TRANSFORM_DFS']
        self.LOAD_DFS = scheduleConfig['LOAD_DFS']
