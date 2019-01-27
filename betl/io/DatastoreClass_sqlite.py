import sqlite3
from .DatastoreClass import Datastore


class SqliteDatastore(Datastore):

    def __init__(self, dbId, path, filename, isSrcSys=False):

        Datastore.__init__(self,
                           datastoreID=dbId,
                           datastoreType='SQLITE',
                           isSrcSys=isSrcSys)

        self.dbId = dbId
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
