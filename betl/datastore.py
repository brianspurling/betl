import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sqlalchemy
import sqlite3


class Datastore():

    def __init__(self, datastoreID, datastoreType, isSrcSys):

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
        readOnlyString = ''
        if isSrcSys:
            readOnlyString = '?mode=ro'
        self.conn = sqlite3.connect(path + '/' + filename + readOnlyString,
                                    uri=True)

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

    def __init__(self, ssID, apiUrl, apiKey, filename, isSrcSys=False):

        Datastore.__init__(self,
                           datastoreID=ssID,
                           datastoreType='SPREADSHEET',
                           isSrcSys=isSrcSys)

        self.ssID = ssID
        self.apiUrl = apiUrl
        self.apiKey = apiKey
        self.filename = filename
        self.conn = self.getGsheetConnection()
        self.worksheets = self.getWorksheets()

    def getGsheetConnection(self):
        client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                self.apiKey,
                self.apiUrl))
        return client.open(self.filename)
        # except SpreadsheetNotFound:
        #     log.error('Failed to establish a connection to the ETL Schema ' +
        #               'spreadsheet: ' + conf.ETL_DB_SCHEMA_FILE_NAME + '. ' +
        #               'This doc should be a Google Doc, matching this name '+
        #               'exactly, and shared to the user in your Google API ' +
        #               'auth file (client_email)')
        #     raise

    def getWorksheets(self):
        worksheets = {}
        for ws in self.conn.worksheets():
            worksheets[ws.title] = ws
        return worksheets

    def __str__(self):
        string = ('\n\n' + '*** Datastore: ' +
                  self.ssID + ' (' + self.datastoreType + ', ' +
                  self.filename + ')' + ' ***' '\n')
        string += '  - worksheets: ' + '\n'
        for wsTitle in self.worksheets:
            string += '    - ' + wsTitle + '\n'
        return string
