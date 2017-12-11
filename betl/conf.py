import logging as logging

# Global variables
BULK_OR_DELTA = None
USE_DEFAULT_EXTRACT = False
LOG_LEVEL = logging.INFO

#
# App-specific settings, assigned by the calling application
#

# General
DWH_ID = None

# Connection details
ETL_DB_CONNS = None
SOURCE_SYSTEM_CONNS = None

# Source to Target Mapping document
STM_FILE_NAME = None
GOOGLE_SHEETS_API_URL = None
GOOGLE_SHEETS_API_KEY_FILE = None

# Connection objects (these are set by functions in utils, called by control)
STM_CONN = None
CTL_DB_CONN = None
ETL_DB_CONN = None
ETL_DB_ENG = None


def loadAppConfig(conf):
    global DWH_ID
    global ETL_DB_CONNS
    global SOURCE_SYSTEM_CONNS
    global STM_FILE_NAME
    global GOOGLE_SHEETS_API_URL
    global GOOGLE_SHEETS_API_KEY_FILE

    DWH_ID = conf['DWH_ID']
    ETL_DB_CONNS = conf['ETL_DB_CONNS']
    SOURCE_SYSTEM_CONNS = conf['SOURCE_SYSTEM_CONNS']
    STM_FILE_NAME = conf['STM_FILE_NAME']
    GOOGLE_SHEETS_API_URL = conf['GOOGLE_SHEETS_API_URL']
    GOOGLE_SHEETS_API_KEY_FILE = conf['GOOGLE_SHEETS_API_KEY_FILE']


def useDefaultExtract(useDefaultExtract=True):
    global USE_DEFAULT_EXTRACT
    USE_DEFAULT_EXTRACT = useDefaultExtract
