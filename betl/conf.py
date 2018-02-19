import logging as logging
import datetime
import os

# Global variables
BULK_OR_DELTA = None
LOG_LEVEL = logging.INFO

#
# App-specific settings, assigned by the calling application
#

# General
DWH_ID = None
TMP_DATA_PATH = None
STAGE = None  # Tells us whether we're on Extract, Transform or Load
LOG_PATH = None

# Connection details
CTL_DB_CONN_DETAILS = None
ETL_DB_CONN_DETAILS = None
TRG_DB_CONN_DETAILS = None
SOURCE_SYSTEM_CONNS = None

# Google Sheets API
GOOGLE_SHEETS_API_URL = None
GOOGLE_SHEETS_API_KEY_FILE = None

ETL_DB_SCHEMA_FILE_NAME = None  # ETL database schema spreadsheet
TRG_DB_SCHEMA_FILE_NAME = None  # TRG database schema spreadsheet
MSD_FILE_NAME = None  # Manual source data spreadsheet

# Connection objects (these are set by functions in utils, called by control)

# Spreadsheets
ETL_DB_SCHEMA_CONN = None
TRG_DB_SCHEMA_CONN = None
MSD_CONN = None  # Manual source data spreadsheet

# Databases
CTL_DB_CONN = None
ETL_DB_CONN = None
ETL_DB_ENG = None
TRG_DB_CONN = None
TRG_DB_ENG = None

# Job execution parameters
SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT = []
TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD = {}
TMP_FILE_SUBDIR_MAPPING = {}

# These dictate the start/end dates of dm_date. They can be overridden at
# any point in the application's ETL process, providing the generateDMDate
# function is added to the schedule _after_ the functions in which they're set
EARLIEST_DATE_IN_DATA = datetime.date(1900, 1, 1)
LATEST_DATE_IN_DATA = datetime.date.today() + datetime.timedelta(days=365)


def loadAppConfig(appConf):
    global DWH_ID

    global CTL_DB_CONN_DETAILS
    global ETL_DB_CONN_DETAILS
    global TRG_DB_CONN_DETAILS
    global SOURCE_SYSTEM_CONNS

    global GOOGLE_SHEETS_API_URL
    global GOOGLE_SHEETS_API_KEY_FILE
    global ETL_DB_SCHEMA_FILE_NAME
    global TRG_DB_SCHEMA_FILE_NAME
    global MSD_FILE_NAME

    global TMP_DATA_PATH
    global LOG_PATH

    DWH_ID = appConf['DWH_ID']

    CTL_DB_CONN_DETAILS = appConf['CTL_DB_CONN']
    ETL_DB_CONN_DETAILS = appConf['ETL_DB_CONN']
    TRG_DB_CONN_DETAILS = appConf['TRG_DB_CONN']
    SOURCE_SYSTEM_CONNS = appConf['SOURCE_SYSTEM_CONNS']

    GOOGLE_SHEETS_API_URL = appConf['GOOGLE_SHEETS_API_URL']
    GOOGLE_SHEETS_API_KEY_FILE = appConf['GOOGLE_SHEETS_API_KEY_FILE']

    ETL_DB_SCHEMA_FILE_NAME = appConf['ETL_DB_SCHEMA_FILE_NAME']
    TRG_DB_SCHEMA_FILE_NAME = appConf['TRG_DB_SCHEMA_FILE_NAME']
    MSD_FILE_NAME = appConf['MSD_FILE_NAME']

    TMP_DATA_PATH = appConf['TMP_DATA_PATH']
    LOG_PATH = appConf['LOG_PATH']


def setEarliestDate(earliestDate):
    global EARLIEST_DATE_IN_DATA
    EARLIEST_DATE_IN_DATA = earliestDate


def setLatestDate(latestDate):
    global LATEST_DATE_IN_DATA
    LATEST_DATE_IN_DATA = latestDate


def getEtlDBEng():
    global ETL_DB_ENG
    return ETL_DB_ENG


def isBulkOrDelta():
    global BULK_OR_DELTA
    return BULK_OR_DELTA


def rebuildTmeFileSubdirMapping():
    global TMP_FILE_SUBDIR_MAPPING
    global TMP_DATA_PATH
    for path, subdirs, files in os.walk(TMP_DATA_PATH):
        for name in files:
            TMP_FILE_SUBDIR_MAPPING[name.replace('.csv', '')] = path.replace(
                TMP_DATA_PATH, '')
