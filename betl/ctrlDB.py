import sys
import psycopg2

from .datastore import PostgresDatastore
from . import logger as logger


class CtrlDB():

    def __init__(self, dbConfigObj):

        self.datastore = \
            PostgresDatastore(
                dbID='CTL',
                host=dbConfigObj['HOST'],
                dbName=dbConfigObj['DBNAME'],
                user=dbConfigObj['USER'],
                password=dbConfigObj['PASSWORD'],
                createIfNotFound=True)

    def createExecutionsTable(self, ):

        ctlDBCursor = self.datastore.cursor()

        ctlDBCursor.execute(
            "CREATE TABLE executions (" +
            "exec_id integer primary key, " +
            "start_datetime timestamp without time zone NOT NULL, " +
            "end_datetime timestamp without time zone, " +
            "status text NOT NULL, " +
            "status_message text, " +
            "bulk_or_delta text NOT NULL, " +
            "limited_data boolean, " +
            "log_file text)")

        self.datastore.commit()

    def createFunctionsTable(self, ):

        ctlDBCursor = self.datastore.cursor()

        ctlDBCursor.execute(
            "CREATE TABLE functions (" +
            "function_id SERIAL PRIMARY KEY, " +
            "exec_id integer REFERENCES executions (exec_id), " +
            "function_name text NOT NULL, " +
            "sequence integer NOT NULL, " +
            "stage text NOT NULL, " +
            "status text NOT NULL, " +
            "start_datetime timestamp without time zone, " +
            "end_datetime timestamp without time zone, " +
            "log text, " +
            "UNIQUE(exec_id, function_name))")

        self.datastore.commit()

    def createDataflowsTable(self, ):

        ctlDBCursor = self.datastore.cursor()

        ctlDBCursor.execute(
            "CREATE TABLE dataflows (" +
            "dataflow_id SERIAL PRIMARY KEY, " +
            "exec_id integer REFERENCES executions (exec_id), " +
            "function_id integer REFERENCES functions (function_id), " +
            "description text NOT NULL, " +
            "status text NOT NULL, " +
            "row_count integer, " +
            "col_count integer, " +
            "start_datetime timestamp without time zone, " +
            "end_datetime timestamp without time zone, " +
            "log text, " +
            "UNIQUE(exec_id, description))")  # Controversial!
        # TODO: at the very least, handle duplicate dfl descs gracefully.
        # I do actually need this (or its equiv) because we need to be able
        # to find all identical dataflows across all execs

        self.datastore.commit()

    def createStepsTable(self, ):

        ctlDBCursor = self.datastore.cursor()

        ctlDBCursor.execute(
            "CREATE TABLE steps (" +
            "step_id SERIAL PRIMARY KEY, " +
            "exec_id integer REFERENCES executions (exec_id), " +
            "dataflow_id integer REFERENCES dataflows (dataflow_id), " +
            "description text, " +
            "status text NOT NULL, " +
            "row_count integer, " +
            "col_count integer, " +
            "start_datetime timestamp without time zone, " +
            "end_datetime timestamp without time zone, " +
            "log text, " +
            "UNIQUE(exec_id, dataflow_id, description))")
        # TODO: at the very least, handle duplicate steps descs gracefully.

        self.datastore.commit()

    def insertExecution(self, execId, bulkOrDelta, limitedData):

        ctlDBCursor = self.datastore.cursor()

        if limitedData is None:
            limitedData = 'FALSE'
        else:
            limitedData = 'TRUE'

        ctlDBCursor.execute(
            "INSERT " +
            "INTO executions (" +
            "exec_id, " +
            "start_datetime, " +
            "end_datetime, " +
            "status, " +
            "status_message, " +
            "bulk_or_delta, " +
            "limited_data) " +
            "VALUES (" +
            str(execId) + ", "
            "current_timestamp, " +
            "NULL, " +
            "'PENDING', " +
            "NULL, " +
            "'" + bulkOrDelta + "', " +
            limitedData + ")")

        self.datastore.commit()

    def insertFunctions(self, functions, execId):

        # Unlike executions, dataflows, and steps, we do not insert functions
        # at the point of execution. Instead, we add them all upfront then
        # update them as we work through the ETL job
        ctlDBCursor = self.datastore.cursor()

        for functionName in functions:
            stage = functions[functionName]['stage']
            sql = ("INSERT " +
                   "INTO functions ("
                   "exec_id, " +
                   "function_name, " +
                   "sequence, " +
                   "stage, " +
                   "status, " +
                   "start_datetime, " +
                   "end_datetime, " +
                   "log) " +
                   "VALUES (" +
                   str(execId) + ", " +
                   "'" + functionName + "', " +
                   "'" + str(functions[functionName]['sequence']) + "', " +
                   "'" + stage + "', " +
                   "'PENDING', " +
                   "NULL, " +
                   "NULL, " +
                   "NULL)")
            ctlDBCursor.execute(sql)

        self.datastore.commit()

    def insertDataflow(self, dataflow):

        ctlDBCursor = self.datastore.cursor()

        ctlDBCursor.execute(
            "INSERT " +
            "INTO dataflows (" +
            "exec_id, " +
            "function_id, " +
            "description, " +
            "status, " +
            "start_datetime, " +
            "end_datetime, " +
            "log) " +
            "VALUES (" +
            "'" + str(dataflow['execId']) + "', " +
            "'" + str(dataflow['functionId']) + "', " +
            "'" + dataflow['description'].replace("'", '"') + "', " +
            "'STARTED', " +
            "current_timestamp, " +
            "NULL, " +
            "NULL)")

        self.datastore.commit()

        ctlDBCursor.execute("SELECT max(dataflow_id) FROM dataflows")
        dataflowId = ctlDBCursor.fetchall()
        return dataflowId[0][0]

    def insertStep(self, step):

        ctlDBCursor = self.datastore.cursor()

        if step['description'] is None:
            desc = 'NULL'
        else:
            desc = "'" + step['description'].replace("'", '"') + "'"

        ctlDBCursor.execute(
            "INSERT " +
            "INTO steps (" +
            "exec_id, " +
            "dataflow_id, " +
            "description, " +
            "status, " +
            "start_datetime, " +
            "end_datetime, " +
            "log) " +
            "VALUES (" +
            str(step['execId']) + ", " +
            str(step['dataflowID']) + ", " +
            desc + ", " +
            "'STARTED', " +
            "current_timestamp, " +
            "NULL, " +
            "NULL)")

        self.datastore.commit()

        ctlDBCursor.execute("SELECT max(step_id) FROM steps")
        stepId = ctlDBCursor.fetchall()
        return stepId[0][0]

    def updateExecution(self,
                        execId,
                        status,
                        statusMessage=''):

        statusMessage = str(statusMessage).replace("'", "").replace('"', '')

        sql = ("UPDATE executions " +
               "SET " +
               "end_datetime = current_timestamp, " +
               "status = '" + status + "', " +
               "log_file = '" + 'placeholder' + "', " +  # TODO!
               "status_message = '" + statusMessage + "' " +
               "WHERE exec_id = " + str(execId))

        ctlDBCursor = self.datastore.cursor()
        ctlDBCursor.execute(sql)
        self.datastore.commit()

    def updateFunction(self,
                       execId,
                       functionName,
                       status,
                       logStr='',
                       setStartDateTime=False,
                       setEndDateTime=False):

        sql = ("UPDATE functions " +
               "SET " +
               "log = '" + logStr.replace("'", "''") + "', ")

        if setStartDateTime:
            sql += "start_datetime = current_timestamp, "
        if setEndDateTime:
            sql += "end_datetime = current_timestamp, "

        sql += "status = '" + status + "'"

        sql += "WHERE exec_id = " + str(execId) + " "
        sql += "  AND function_name = '" + functionName + "' "

        ctlDBCursor = self.datastore.cursor()
        ctlDBCursor.execute(sql)
        self.datastore.commit()

    def updateDataflow(self,
                       dataflowId,
                       status,
                       rowCount=None,
                       colCount=None,
                       logStr=''):

        sql = ("UPDATE dataflows " +
               "SET " +
               "log = '" + logStr.replace("'", "''") + "', ")

        if rowCount is not None:
            sql += "row_count = " + str(rowCount) + ", "
        if colCount is not None:
            sql += "col_count = " + str(colCount) + ", "
        sql += "end_datetime = current_timestamp, "

        sql += "status = '" + status + "'"

        sql += "WHERE dataflow_id = " + str(dataflowId) + " "

        ctlDBCursor = self.datastore.cursor()
        ctlDBCursor.execute(sql)
        self.datastore.commit()

    def updateStep(self,
                   stepId,
                   status,
                   rowCount=None,
                   colCount=None,
                   logStr=''):

        sql = ("UPDATE steps " +
               "SET " +
               "log = '" + logStr.replace("'", "''") + "', ")

        if rowCount is not None:
            sql += "row_count = " + str(rowCount) + ", "
        if colCount is not None:
            sql += "col_count = " + str(colCount) + ", "
        sql += "end_datetime = current_timestamp, "

        sql += "status = '" + status + "'"

        sql += "WHERE step_id = " + str(stepId) + " "

        ctlDBCursor = self.datastore.cursor()
        ctlDBCursor.execute(sql)
        self.datastore.commit()

    def getLastExecution(self):

        ctlDBCursor = self.datastore.cursor()

        try:
            ctlDBCursor.execute("SELECT exec_id, status FROM executions " +
                                "WHERE exec_id = (select max(exec_id) " +
                                "FROM executions)")
        except psycopg2.Error as e:
            # We have to print here, because we don't have an execId yet to
            # be able to use the logger
            print(logger.logUnableToReadFromCtlDB(e.pgerror))
            sys.exit()

        return ctlDBCursor.fetchall()

    def getFunctionsForExec(self, execId):

        ctlDBCursor = self.datastore.cursor()
        ctlDBCursor.execute(
            "SELECT function_id, " +
            "       exec_id, " +
            "       function_name, " +
            "       sequence, " +
            "       stage, " +
            "       status, " +
            "       start_datetime, " +
            "       end_datetime, " +
            "       log " +
            "FROM   functions " +
            "WHERE  exec_id = " + str(execId) + " " +
            "ORDER BY sequence ASC")
        return ctlDBCursor.fetchall()

    def dropAllCtlTables(self, ):

        ctlDBCursor = self.datastore.cursor()

        ctlDBCursor.execute("SELECT * " +
                            "FROM   information_schema.tables t " +
                            "WHERE  t.table_schema = 'public'")

        controlTables = ctlDBCursor.fetchall()

        counter = 0
        for controlTable in controlTables:
            counter += 1
            ctlDBCursor.execute("DROP TABLE " + controlTable[2] + " CASCADE")

        self.datastore.commit()
