from . import conf

import logging as logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
# For clearing out the temp data folder
import os
import tempfile
import shutil
import pprint


def setUpLogger(moduleCode, name):
    logger = logging.getLogger(name)
    syslog = logging.StreamHandler()
    formatter = logging.Formatter('%(module_code)s.%(funcName)s: %(message)s')
    syslog.setFormatter(formatter)
    logger.setLevel(conf.LOG_LEVEL)
    logger.addHandler(syslog)
    return logging.LoggerAdapter(logger, {'module_code': moduleCode})


log = setUpLogger(' UTILS', __name__)


#
# Functions to connect to the DBs (CTL, ETL and TRG) and STM
#
def getCtlDBConnection(reload=False):
    if conf.CTL_DB_CONN is None or reload:
        conf.CTL_DB_CONN = getDBConnection('ctl')
    return conf.CTL_DB_CONN


def getEtlDBConnection(reload=False):
    if conf.ETL_DB_CONN is None or reload:
        conf.ETL_DB_CONN = getDBConnection('etl')
    return conf.ETL_DB_CONN


def getTrgDBConnection(reload=False):
    if conf.TRG_DB_CONN is None or reload:
        conf.TRG_DB_CONN = getDBConnection('trg')
    return conf.TRG_DB_CONN


def getDBConnection(connId):

    # If we don't have a connection yet, connect to the postgres instance,
    # and check whether the DB exists. If it doesn't, create it.
    # THEN connect to the DB
    log.debug(connId + " DB connection does not yet exist, " +
              "attempting to connect")

    # Get the correct connection details from conf
    connDetails = {}
    if connId == 'ctl':
        connDetails = conf.CTL_DB_CONN_DETAILS
    elif connId == 'etl':
        connDetails = conf.ETL_DB_CONN_DETAILS
    elif connId == 'trg':
        connDetails = conf.ETL_DB_CONN_DETAILS

    # Connect to the DB server, without specifiying a database. We're going
    # to first check whether the DB exists
    dbServerConnectionString = 'host='                                    \
        + connDetails['HOST']                                             \
        + ' dbname=postgres user='                                        \
        + connDetails['USER']                                             \
        + ' password='                                                    \
        + connDetails['PASSWORD']
    dbServerConn = psycopg2.connect(dbServerConnectionString)
    dbServerConn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    dbServerCursor = dbServerConn.cursor()
    dbServerCursor.execute("SELECT * FROM pg_database WHERE datname = '" +
                           conf.DWH_ID + "_" + connId + "'")
    dbs = dbServerCursor.fetchall()

    if(len(dbs) == 0):
        log.info("the " + connId + " database does not exist, creating")
        dbServerCursor.execute('CREATE DATABASE '
                               + conf.DWH_ID + '_'
                               + connId)
        log.info("an empty " + connId + " database has been created")

    # Reconstruct the connection string, this time with the DB name
    dbConnectionString = 'host='                                          \
        + connDetails['HOST']                               \
        + ' dbname=' + connDetails['DBNAME']                \
        + ' user=' + connDetails['USER']                    \
        + ' password=' + connDetails['PASSWORD']
    connection = psycopg2.connect(dbConnectionString)

    log.info("Connected to " + connId + " DB")

    return connection


def getEtlDBEngine(reload=False):
    log.debug('START')

    if conf.ETL_DB_ENG is None or reload:
        conf.ETL_DB_ENG = create_engine(r'postgresql://'
                                        + conf.ETL_DB_CONN_DETAILS['USER']
                                        + ':@'
                                        + conf.ETL_DB_CONN_DETAILS['HOST']
                                        + '/'
                                        + conf.ETL_DB_CONN_DETAILS['DBNAME'])
    log.debug('END')

    return conf.ETL_DB_ENG


def getTrgDBEngine(reload=False):
    log.debug('START')

    if conf.TRG_DB_ENG is None or reload:
        conf.TRG_DB_ENG = create_engine(r'postgresql://'
                                        + conf.TRG_DB_CONN_DETAILS['USER']
                                        + ':@'
                                        + conf.TRG_DB_CONN_DETAILS['HOST']
                                        + '/'
                                        + conf.TRG_DB_CONN_DETAILS['DBNAME'])
    log.debug('END')

    return conf.TRG_DB_ENG


#
# Returns the connection to the STM (source to target mapping) document,
# which is a Google Spreadsheet.
# The returned value is a gspread object that allows access to the GSheet,
# via Google's API
#
def getStmConnection(reload=False):

    log.debug("START")

    if conf.STM_CONN is None or reload:
        client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                conf.GOOGLE_SHEETS_API_KEY_FILE,
                [conf.GOOGLE_SHEETS_API_URL]))
        conf.STM_CONN = client.open(conf.STM_FILE_NAME)

    log.info("Connected to STM Google Sheet")

    return conf.STM_CONN


#
# Returns the connection to the Manual Source Data spreadsheet
# The returned value is a gspread object that allows access to the GSheet,
# via Google's API
#
def getMsdConnection(reload=False):

    log.debug("START")

    if conf.MSD_CONN is None or reload:
        client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                conf.GOOGLE_SHEETS_API_KEY_FILE,
                [conf.GOOGLE_SHEETS_API_URL]))
        conf.MSD_CONN = client.open(conf.MSD_FILE_NAME)

    log.info("Connected to Manual Source Data Google Sheet")

    return conf.MSD_CONN


#
# Get all the worksheets from the STM GSheet document, then
# filter to only those relevant to this Data Layer
#
def getStmWorksheets(dataLayer):

    allStmWorksheets = conf.STM_CONN.worksheets()
    worksheets = []

    if dataLayer == 'src':
        for worksheet in allStmWorksheets:
            if worksheet.title.find('ETL.SRC.') > -1:
                worksheets.append(worksheet)
    else:
        # this needs to do something differnet for other layers, e.g. TRG
        # will have DM_ and FT_ worksheets
        raise ValueError('code not written yet')

    return worksheets


#
# Get all the worksheets from the Manual Source Data GSheet document, then
#
def getMsdWorksheets():

    return conf.MSD_CONN.worksheets()


#
# Functions to set the audit columns on the dataframes, prior to loading
# into persistent storage
#
def setAuditCols(df, sourceSystemId, action):

    log.debug("START")

    if action == 'BULK':
        return setAuditCols_bulk(df, sourceSystemId)
    elif action == 'INSERT':
        return setAuditCols_bulk(df, sourceSystemId)
    elif action == 'UPDATE':
        return setAuditCols_bulk(df, sourceSystemId)
    elif action == 'DELETE':
        return setAuditCols_bulk(df)
    else:
        raise ValueError("Incorrect parameter action passed to setAuditCols")


def setAuditCols_bulk(df, sourceSystemId):

    log.debug("START")

    df['audit_source_system'] = sourceSystemId
    df['audit_bulk_load_date'] = datetime.datetime.now()
    df['audit_latest_delta_load_date'] = None
    df['audit_latest_delta_load_operation'] = None

    log.debug("END")

    return df


def setAuditCols_insert(df, sourceSystemId):

    log.debug("START")

    df['audit_source_system'] = sourceSystemId
    df['audit_bulk_load_date'] = None
    df['audit_latest_delta_load_date'] = datetime.datetime.now()
    df['audit_latest_delta_load_operation'] = 'INSERT'

    log.debug("END")

    return df


def setAuditCols_update(df, sourceSystemId):

    log.debug("START")

    df['audit_source_system'] = sourceSystemId
    df['audit_latest_delta_load_date'] = datetime.datetime.now()
    df['audit_latest_delta_load_operation'] = 'UPDATE'

    log.debug("END")

    return df


def setAuditCols_delete(df):

    log.debug("START")

    df['audit_latest_delta_load_date'] = datetime.datetime.now()
    df['audit_latest_delta_load_operation'] = 'DELETE'

    log.debug("END")

    return df


def deleteTempoaryData():
    log.debug("START")

    path = conf.TMP_DATA_PATH

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

    log.debug("END")


def describeDF(funcName, stepDescription, df, stepId):
    print('\n')
    print('******************************************************************')
    print('\n')
    print(funcName + ': Step ' + str(stepId))
    print(stepDescription)
    print('Shape: ' + str(df.shape))
    print('\n')
    pprint.pprint('Columns: ' + str(list(df.columns.values)))
    if len(df.columns.values) < 4:
        print('\n')
        pprint.pprint(df.head())
