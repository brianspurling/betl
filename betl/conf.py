import logging as logging

# Global variables
BULK_OR_DELTA = None
LOG_LEVEL = logging.INFO

#
# App-specific settings, assigned by the calling application
#

# General
DWH_ID = None

# Connection details
CTL_DB_CONN_DETAILS = None
ETL_DB_CONN_DETAILS = None
TRG_DB_CONN_DETAILS = None
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
TRG_DB_CONN = None
TRG_DB_ENG = None



def loadAppConfig(appConf):
    global DWH_ID

    global CTL_DB_CONN_DETAILS
    global ETL_DB_CONN_DETAILS
    global TRG_DB_CONN_DETAILS
    global SOURCE_SYSTEM_CONNS

    global STM_FILE_NAME
    global GOOGLE_SHEETS_API_URL
    global GOOGLE_SHEETS_API_KEY_FILE

    DWH_ID = appConf['DWH_ID']

    CTL_DB_CONN_DETAILS = appConf['CTL_DB_CONN']
    ETL_DB_CONN_DETAILS = appConf['ETL_DB_CONN']
    TRG_DB_CONN_DETAILS = appConf['TRG_DB_CONN']
    SOURCE_SYSTEM_CONNS = appConf['SOURCE_SYSTEM_CONNS']

    STM_FILE_NAME = appConf['STM_FILE_NAME']
    GOOGLE_SHEETS_API_URL = appConf['GOOGLE_SHEETS_API_URL']
    GOOGLE_SHEETS_API_KEY_FILE = appConf['GOOGLE_SHEETS_API_KEY_FILE']

