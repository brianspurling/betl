from . import logger

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sqlalchemy
import sqlite3
from apiclient.discovery import build
import httplib2


class Datastore():

    def __init__(self,
                 datastoreID,
                 datastoreType,
                 isSrcSys,
                 isSchemaDesc=False):

        if not isSrcSys:
            logger.logInitialiseDatastore(
                datastoreID,
                datastoreType,
                isSchemaDesc)

        self.datatoreID = datastoreID
        self.datastoreType = datastoreType
        self.isSrcSys = isSrcSys


class PostgresDatastore(Datastore):

    def __init__(self, dbID, host, dbName, user, password,
                 createIfNotFound=False,
                 isSrcSys=False):

        Datastore.__init__(self,
                           datastoreID=dbID,
                           datastoreType='POSTGRES',
                           isSrcSys=isSrcSys)

        self.dbID = dbID
        self.host = host
        self.dbName = dbName
        self.user = user
        self.password = password
        self.conn = self.getDBConnection(createIfNotFound, isSrcSys)
        self.eng = sqlalchemy.create_engine(r'postgresql://'
                                            + self.user
                                            + ':@'
                                            + self.host
                                            + '/'
                                            + self.dbName)

    def commit(self):
        self.conn.commit()

    def cursor(self):
        return self.conn.cursor()

    def getDBConnection(self, createIfNotFound, isSrcSys):
        # We will temporarily connect to the postgres database, to check
        # whether configDetails['DBNAME'] exists yet

        tempConnectionString = "host='" + self.host + "' "\
                               "dbname='postgres' " + \
                               "user='" + self.user + "' " + \
                               "password='" + self.password + "' "

        tempConn = psycopg2.connect(tempConnectionString)
        tempDBCursor = tempConn.cursor()
        tempDBCursor.execute("SELECT * FROM pg_database " +
                             "WHERE datname = '" + self.dbName + "'")
        dbs = tempDBCursor.fetchall()

        if(len(dbs) == 0 and createIfNotFound):
            tempConn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            tempDBCursor.execute('CREATE DATABASE ' + self.dbName)

        connectionString = "host='" + self.host + "'" + \
                           "dbname='" + self.dbName + "'" + \
                           "user='" + self.user + "'" + \
                           "password='" + self.password + "'"

        conn = psycopg2.connect(connectionString)

        if isSrcSys:
            conn.set_session(readonly=True)

        return conn


class SqliteDatastore(Datastore):

    def __init__(self, dbID, path, filename, isSrcSys=False):

        Datastore.__init__(self,
                           datastoreID=dbID,
                           datastoreType='SQLITE',
                           isSrcSys=isSrcSys)

        self.dbID = dbID
        self.path = path
        self.filename = filename
        # NOTE: you're supposed to be able to connect in read-only mode using:
        # readOnlyString = ''
        # if isSrcSys:
        #    readOnlyString = '?mode=ro'
        # self.conn = sqlite3.connect('file:/' + path + filename +
        #                             readOnlyString,
        #                             uri=True)
        # But it doesn't seem to work - just tries to open a database of that
        # full name (including query string). See:
        # https://stackoverflow.com/questions/10205744/
        #   opening-sqlite3-database-from-python-in-read-only-mode

        self.conn = sqlite3.connect(path + filename)

    def commit(self):
        self.conn.commit()

    def cursor(self):
        return self.conn.cursor()


class FileDatastore(Datastore):

    def __init__(self, fileSysID, path, fileExt, delim, quotechar,
                 isSrcSys=False):

        Datastore.__init__(self,
                           datastoreID=fileSysID,
                           datastoreType='FILESYSTEM',
                           isSrcSys=isSrcSys)

        self.fileSysID = fileSysID
        self.path = path
        self.fileExt = fileExt
        self.delim = delim
        self.quotechar = quotechar


class SpreadsheetDatastore(Datastore):

    def __init__(self,
                 ssID,
                 apiUrl,
                 apiKey,
                 filename,
                 isSrcSys=False,
                 isSchemaDesc=False):

        Datastore.__init__(self,
                           datastoreID=ssID,
                           datastoreType='GSHEET',
                           isSrcSys=isSrcSys,
                           isSchemaDesc=isSchemaDesc)

        # TODO don't do all this on init, it slows down the start
        # of the job, which has a diproportionate effect on dev time.
        self.ssID = ssID
        self.apiUrl = apiUrl
        self.apiKey = apiKey
        self.filename = filename
        self.conn = self.getGsheetConnection()
        self.worksheets = self.getWorksheets()
        self.gdriveConn = self.getGdriveConnection()
        self.lastModifiedTime = self.getLastModifiedTime()

    def getGsheetConnection(self):
        _client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                self.apiKey,
                self.apiUrl))
        return _client.open(self.filename)
        # except SpreadsheetNotFound:
        #     log.error('Failed to establish a connection to the ETL Schema ' +
        #               'spreadsheet: ' + conf.ETL_DB_SCHEMA_FILE_NAME + '. ' +
        #               'This doc should be a Google Doc, matching this name '+
        #               'exactly, and shared to the user in your Google API ' +
        #               'auth file (client_email)')
        #     raise

    def getGdriveConnection(self):
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.apiKey,
            'https://www.googleapis.com/auth/drive.metadata.readonly')
        return build('drive', 'v3', http=creds.authorize(httplib2.Http()))

    def getWorksheets(self):
        worksheets = {}
        for ws in self.conn.worksheets():
            worksheets[ws.title] = ws
        self.worksheets = worksheets
        return worksheets

    def getLastModifiedTime(self):
        result = self.gdriveConn.files().get(
            fileId=self.conn.id,
            fields='modifiedTime').execute()
        return result['modifiedTime']

    def __str__(self):
        string = ('\n\n' + '*** Datastore: ' +
                  self.ssID + ' (' + self.datastoreType + ', ' +
                  self.filename + ')' + ' ***' '\n')
        string += '  - worksheets: ' + '\n'
        for wsTitle in self.worksheets:
            string += '    - ' + wsTitle + '\n'
        return string
