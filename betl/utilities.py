from . import conf

import logging as logging

import gspread
from gspread import SpreadsheetNotFound
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
# Functions to connect to the DBs (CTL, ETL and TRG) & spreadsheets (ETL, TRG)
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
        connDetails = conf.TRG_DB_CONN_DETAILS

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
# Returns the connection to the ETL Schema document, which is a Google sheet
#
# to do #21
def getEtlSchemaConnection(reload=False):

    log.debug("START")

    if conf.ETL_DB_SCHEMA_CONN is None or reload:
        client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                conf.GOOGLE_SHEETS_API_KEY_FILE,
                [conf.GOOGLE_SHEETS_API_URL]))

        try:
            conf.ETL_DB_SCHEMA_CONN = client.open(conf.ETL_DB_SCHEMA_FILE_NAME)
        except SpreadsheetNotFound:
            log.error('Failed to establish a connection to the ETL Schema ' +
                      'spreadsheet: ' + conf.ETL_DB_SCHEMA_FILE_NAME + '. ' +
                      'This doc should be a Google Doc, matching this name ' +
                      'exactly, and shared to the user in your Google API ' +
                      'auth file (client_email)')
            raise

    log.info("Connected to ETL DB Schema Google Sheet")

    return conf.ETL_DB_SCHEMA_CONN


#
# Returns the connection to the TRG Schema document, which is a Google sheet
#
# to do #21
def getTrgSchemaConnection(reload=False):

    log.debug("START")

    if conf.TRG_DB_SCHEMA_CONN is None or reload:
        client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                conf.GOOGLE_SHEETS_API_KEY_FILE,
                [conf.GOOGLE_SHEETS_API_URL]))
        conf.TRG_DB_SCHEMA_CONN = client.open(conf.TRG_DB_SCHEMA_FILE_NAME)

    log.info("Connected to TRG DB Schema Google Sheet")

    return conf.TRG_DB_SCHEMA_CONN


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
# Get all the worksheets from the appropriate DB Schema document, then
# filter to only those relevant to this dataLase / dataLayer. There are two
# DB Schema documents, covering the following dataLayers
#   etl.src     --> one dataModel per source system (and one for MSD)
#   etl.stg     --> one dataModel per stage of the ETL staging process
#   trg.trg     --> one dataModel only, called TRG as well (but prefixed ft/dm)
#   trg.sum     --> one dataModel only, called SUM as well (but prefixed su)
#
# Do not specify an etlDataLayer if you are requesting the TRG database - you
# always get all worksheets for all dataLayers for TRG
#
def getSchemaWorksheets(database, etlDataLayer=None):

    worksheets = []

    if database == 'etl' and etlDataLayer == 'src':
        allEtlSchemaWorksheets = conf.ETL_DB_SCHEMA_CONN.worksheets()
        for worksheet in allEtlSchemaWorksheets:
            if worksheet.title.find('ETL.SRC.') > -1:
                worksheets.append(worksheet)

    elif database == 'etl' and etlDataLayer == 'stg':
        allEtlSchemaWorksheets = conf.ETL_DB_SCHEMA_CONN.worksheets()
        for worksheet in allEtlSchemaWorksheets:
            if worksheet.title.find('ETL.SRC.') == -1 and                     \
                    worksheet.title.find('ETL.') > -1:
                worksheets.append(worksheet)

    elif database == 'trg' and etlDataLayer == 'trg':
        allTrgSchemaWorksheets = conf.TRG_DB_SCHEMA_CONN.worksheets()
        for worksheet in allTrgSchemaWorksheets:
            if worksheet.title.find('TRG.TRG.') > -1:
                worksheets.append(worksheet)

    elif database == 'trg' and etlDataLayer == 'sum':
        allTrgSchemaWorksheets = conf.TRG_DB_SCHEMA_CONN.worksheets()
        for worksheet in allTrgSchemaWorksheets:
            if worksheet.title.find('TRG.SUM.') > -1:
                worksheets.append(worksheet)
    else:
        raise ValueError('Invalid combination of database and etlDataLayer: ' +
                         database + ' - ' + etlDataLayer)

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
    op = ''

    op += '\n'
    op += funcName + ': Step ' + str(stepId) + ' - '
    op += stepDescription + '\n\n'
    op += 'Shape: ' + str(df.shape) + '\n'
    op += '\n'
    op += 'Columns: '
    op += pprint.pformat(list(df.columns.values))
    if len(df.columns.values) < 4:
        op += '\n\n'
        op += 'df.head >>> '
        op += '\n\n'
        op += pprint.pformat(df.head())
    op += '\n'
    op += '\n'
    op += '******************************************************************'

    print(op)
    return op
