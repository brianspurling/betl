from . import conf

import logging as logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine


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
# Functions to connect to the DBs (ETL and CTL) and STM
#
def getCtlDBConnection(reload=False):
    if conf.CTL_DB_CONN is None or reload:
        conf.CTL_DB_CONN = getDBConnection('ctl')
    return conf.CTL_DB_CONN


def getEtlDBConnection(reload=False):
    if conf.ETL_DB_CONN is None or reload:
        conf.ETL_DB_CONN = getDBConnection('etl')
    return conf.ETL_DB_CONN


def getDBConnection(connId):

    # If we don't have a connection yet, connect to the postgres instance,
    # and check whether the DB exists. If it doesn't, create it.
    # THEN connect to the DB
    log.debug(connId + " DB connection does not yet exist, " +
              "attempting to connect")

    dbServerConnectionString = 'host='                                    \
        + conf.ETL_DB_CONNS[connId]['HOST']                               \
        + ' dbname=postgres user='                                        \
        + conf.ETL_DB_CONNS[connId]['USER']                               \
        + ' password='                                                    \
        + conf.ETL_DB_CONNS[connId]['PASSWORD']
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
    else:
        pass

    dbConnectionString = 'host='                                          \
        + conf.ETL_DB_CONNS[connId]['HOST']                               \
        + ' dbname=' + conf.ETL_DB_CONNS[connId]['DBNAME']                \
        + ' user=' + conf.ETL_DB_CONNS[connId]['USER']                    \
        + ' password=' + conf.ETL_DB_CONNS[connId]['PASSWORD']
    connection = psycopg2.connect(dbConnectionString)

    log.info("Connected to " + connId + " DB")

    return connection


def getEtlDBEngine(reload=False):
    log.debug('START')

    if conf.ETL_DB_ENG is None or reload:
        conf.ETL_DB_ENG = create_engine(r'postgresql://'
                                        + conf.ETL_DB_CONNS['etl']['USER']
                                        + ':@'
                                        + conf.ETL_DB_CONNS['etl']['HOST']
                                        + '/'
                                        + conf.ETL_DB_CONNS['etl']['DBNAME'])
    log.debug('END')

    return conf.ETL_DB_ENG


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


def getStmWorksheets(dataLayer):

    # Get all the worksheets from the STM GSheet document, then
    # filter to only those relevant to this Data Layer
    allSTMWorksheets = conf.STM_CONN.worksheets()
    worksheets = []

    if dataLayer == 'src':
        for worksheet in allSTMWorksheets:
            if worksheet.title.find('ETL.SRC.') > -1:
                worksheets.append(worksheet)
    else:
        # this needs to do something differnet for other layers, e.g. TRG
        # will have DM_ and FT_ worksheets
        raise ValueError('code not written yet')

    return worksheets


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
