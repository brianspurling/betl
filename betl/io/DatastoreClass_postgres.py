from .DatastoreClass import Datastore
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sqlalchemy


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

    def rollback(self):
        self.conn.rollback()

    def cursor(self):
        return self.conn.cursor()

    def getDBConnection(self, createIfNotFound, isSrcSys):
        # We will temporarily connect to the postgres database, to check
        # whether configDetails['DBNAME'] exists yet

        tempConnectionString = "host='" + self.host + "' "\
                               "dbname='postgres' " + \
                               "user='" + self.user + "' " + \
                               'password="' + self.password + '"'

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
