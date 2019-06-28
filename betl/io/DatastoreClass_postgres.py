from .DatastoreClass import Datastore
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sqlalchemy


class PostgresDatastore(Datastore):

    def __init__(self, dbId, host, dbName, user, password, schema=None,
                 createIfNotFound=False,
                 isSrcSys=False):

        Datastore.__init__(self,
                           datastoreID=dbId,
                           datastoreType='POSTGRES',
                           isSrcSys=isSrcSys)

        self.dbId = dbId
        self.host = host
        self.dbName = dbName
        self.user = user
        self.password = password
        self.schema = schema
        self.conn = self.getDBConnection(createIfNotFound, isSrcSys)

        schemaDict = None
        if schema is not None:
            schemaDict = {'options': '-csearch_path={}'.format(self.schema)}

        self.eng = sqlalchemy.create_engine(
            r'postgresql://'
            + self.user
            + ':' + self.password
            + '@'
            + self.host
            + '/'
            + self.dbName,
            connect_args=schemaDict)

        # @sqlalchemy.event.listens_for(self.eng, 'before_cursor_execute')
        # def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        #     print("Listen before_cursor_execute - executemany: %s" % str(executemany))
        #     if executemany:
        #         cursor.fast_executemany = True
        #         cursor.commit()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def cursor(self):
        return self.conn.cursor()

    def getDBConnection(self, createIfNotFound, isSrcSys):
        # We will temporarily connect to the postgres database, to check
        # whether configDetails['DBNAME'] exists yet

        tempConn = psycopg2.connect(host=self.host,
                                    database='postgres',
                                    user=self.user,
                                    password=self.password)
        tempDBCursor = tempConn.cursor()
        tempDBCursor.execute("SELECT * FROM pg_database " +
                             "WHERE datname = '" + self.dbName + "'")
        dbs = tempDBCursor.fetchall()

        if(len(dbs) == 0 and createIfNotFound):
            tempConn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            tempDBCursor.execute('CREATE DATABASE ' + self.dbName)

        options = None
        if self.schema is not None:
            options = '-c search_path={' + self.schema + '}'

        conn = psycopg2.connect(host=self.host,
                                database=self.dbName,
                                user=self.user,
                                password=self.password,
                                options=options)
        if isSrcSys:
            conn.set_session(readonly=True)

        return conn
