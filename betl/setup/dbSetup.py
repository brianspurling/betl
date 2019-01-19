import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from betl.io import PostgresDatastore
from betl import ControlDB


def createDatabases(self, response):

    if response.lower() in ['y', '']:
        con = psycopg2.connect(
            dbname='postgres',
            user=self.CTL_DB_USERNAME,
            host=self.CTL_DB_HOST_NAME,
            password=self.CTL_DB_PASSWORD)

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS " + self.CTL_DB_NAME + ";")
        cur.execute("CREATE DATABASE " + self.CTL_DB_NAME + ";")

        ctrlDB = ControlDB()

        ctrlDBDatastore = PostgresDatastore(
                    dbId='CTL',
                    host=self.CTRL_DB_DETAILS['host'],
                    dbName=self.CTRL_DB_DETAILS['dbName'],
                    user=self.CTRL_DB_DETAILS['username'],
                    password=self.CTRL_DB_DETAILS['password'],
                    createIfNotFound=True)
        ctrlDB.setCtrlDBConnectionManually(ctrlDBDatastore)

        ctrlDB.createExecutionsTable()
        ctrlDB.createFunctionsTable()
        ctrlDB.createDataflowsTable()
        ctrlDB.createStepsTable()

        print('Control DB: ' + self.CTL_DB_NAME)

        con = psycopg2.connect(
            dbname='postgres',
            user=self.ETL_DB_USERNAME,
            host=self.ETL_DB_HOST_NAME,
            password=self.ETL_DB_PASSWORD)

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS " + self.ETL_DB_NAME + ";")
        cur.execute("CREATE DATABASE " + self.ETL_DB_NAME + ";")

        print('ETL DB: ' + self.ETL_DB_NAME)

        con = psycopg2.connect(
            dbname='postgres',
            user=self.TRG_DB_USERNAME,
            host=self.TRG_DB_HOST_NAME,
            password=self.TRG_DB_PASSWORD)

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS " + self.TRG_DB_NAME + ";")
        cur.execute("CREATE DATABASE " + self.TRG_DB_NAME + ";")

        print('TRG DB: ' + self.TRG_DB_NAME)


def deleteDatabases(self):
    con = psycopg2.connect(
        dbname='postgres',
        user=self.CTL_DB_USERNAME,
        host=self.CTL_DB_HOST_NAME,
        password=self.CTL_DB_PASSWORD)
    cur = con.cursor()
    cur.execute("DROP DATABASE IF EXISTS " + self.CTL_DB_NAME + ";")
    cur.execute("DROP DATABASE IF EXISTS " + self.ETL_DB_NAME + ";")
    cur.execute("DROP DATABASE IF EXISTS " + self.TRG_DB_NAME + ";")
