from . import conf
from . import logger

import inspect

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
import pandas as pd


log = logger.setUpLogger(' UTILS', __name__)


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


def readFromEtlDB(tableName):
    conn = getEtlDBConnection()

    callingFuncName = inspect.stack()[1][3]

    logger.logStepStart('Reading data ' + tableName + ' in ETL DB',
                        callingFuncName=callingFuncName)

    df = pd.read_sql('SELECT * FROM ' + tableName, con=conn)

    # We never want audit cols to come into transform dataframes
    # TODO: I should be more confident that if there's one there's all!
    if 'audit_source_system' in df.columns:
        df.drop(['audit_source_system'], axis=1, inplace=True)
    if 'audit_bulk_load_date' in df.columns:
        df.drop(['audit_bulk_load_date'], axis=1, inplace=True)
    if 'audit_latest_delta_load_date' in df.columns:
        df.drop(['audit_latest_delta_load_date'], axis=1, inplace=True)
    if 'audit_latest_delta_load_operation' in df.columns:
        df.drop(['audit_latest_delta_load_operation'], axis=1, inplace=True)

    logger.logStepEnd(df)

    return df


def readFromSrcDB(tableName, conn, dataModelId):

    callingFuncName = inspect.stack()[1][3]

    logger.logStepStart('Reading data ' +
                        tableName +
                        ' in source DB ' +
                        dataModelId,
                        callingFuncName=callingFuncName)
    df = pd.read_sql('SELECT * FROM ' + tableName, con=conn)
    logger.logStepEnd(df)

    return df


#
# Functions to set the audit columns on the dataframes, prior to loading
# into persistent storage
#
def setAuditCols(df, sourceSystemId, action):

    log.debug("START")
    if action == 'BULK':
        return setAuditCols_bulk(df, sourceSystemId)
    elif action == 'INSERT':
        return setAuditCols_insert(df, sourceSystemId)
    elif action == 'UPDATE':
        return setAuditCols_update(df, sourceSystemId)
    elif action == 'DELETE':
        return setAuditCols_delete(df)
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

    path = conf.TMP_DATA_PATH.replace('/', '')

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


def openFileForAppend(filename):
    return open(
        conf.TMP_DATA_PATH +
        conf.STAGE + '/' +
        filename +
        '.csv',
        'a'
    )


def writeToCsv(df, file_or_filename):

    callingFuncName = inspect.stack()[1][3]

    path = ''
    _file = None
    headers = True
    if type(file_or_filename) is str:
        conf.TMP_FILE_SUBDIR_MAPPING[file_or_filename] = conf.STAGE
        path = (conf.TMP_DATA_PATH + conf.STAGE + '/' +
                file_or_filename + '.csv')
        if not os.path.exists(conf.TMP_DATA_PATH + conf.STAGE + '/'):
            os.makedirs(conf.TMP_DATA_PATH + conf.STAGE + '/')
        _file = open(path, 'w')
    else:
        path = file_or_filename.name
        _file = file_or_filename
        headers = False

    logger.logStepStart('Writing data to CSV: ' + path,
                        callingFuncName=callingFuncName)
    df.to_csv(_file, header=headers, index=False)
    logger.logStepEnd(df)


def readFromCsv(file_or_filename,
                sep=None,
                quotechar=None,
                pathOverride=False):

    callingFuncName = inspect.stack()[1][3]
    _sep = sep if sep is not None else ','
    _quotechar = quotechar if quotechar is not None else '"'
    path = ''
    _file = None

    if type(file_or_filename) is str:
        if (not pathOverride):
            if file_or_filename not in conf.TMP_FILE_SUBDIR_MAPPING:
                conf.rebuildTmeFileSubdirMapping()
            path = (conf.TMP_DATA_PATH +
                    conf.TMP_FILE_SUBDIR_MAPPING[file_or_filename] + '/' +
                    file_or_filename + '.csv')
        else:
            path = file_or_filename
        _file = open(path)
    else:
        path = file_or_filename.name
        _file = file_or_filename

    logger.logStepStart('Reading data from CSV: ' + path,
                        callingFuncName=callingFuncName)
    df = pd.read_csv(_file,
                     sep=_sep,
                     quotechar=_quotechar)
    logger.logStepEnd(df)
    return df
