import sys
import psycopg2

from . import logger as logger


class CtrlDB():

    def __init__(self, ctrlDatastore):

        self.datastore = ctrlDatastore

    def insertNewExecutionToCtlTable(self, execId, bulkOrDelta):
        ctlDBCursor = self.datastore.cursor()

        ctlDBCursor.execute("INSERT " +
                            "INTO executions (" +
                            "exec_id, " +
                            "start_datetime, " +
                            "end_datetime, " +
                            "status, " +
                            "status_message, " +
                            "bulk_or_delta) " +
                            "VALUES (" +
                            str(execId) + ", "
                            "current_timestamp, " +
                            "NULL, " +
                            "'PENDING', " +
                            "NULL, " +
                            "'" + bulkOrDelta + "')")
        self.datastore.commit()

    def insertNewScheduleToCtlTable(self, scheduleDict, execId):

        ctlDBCursor = self.datastore.cursor()

        for dataflow in scheduleDict:
            stage = scheduleDict[dataflow]['stage']
            sql = "INSERT " +                                                 \
                  "INTO schedules (" +                                        \
                  "exec_id, " +                                               \
                  "dataflow_name, " +                                         \
                  "stage, " +                                                 \
                  "status, " +                                                \
                  "start_datetime, " +                                        \
                  "end_datetime, " +                                          \
                  "log) " +                                                   \
                  "VALUES (" +                                                \
                  str(execId) + ", " +                                        \
                  "'" + dataflow + "', " +                                    \
                  "'" + stage + "', " +                                       \
                  "'PENDING', " +                                             \
                  "NULL, " +                                                  \
                  "NULL, " +                                                  \
                  "NULL)"
            ctlDBCursor.execute(sql)

        self.datastore.commit()

    def updateExecutionInCtlTable(self, execId, status, statusMessage=''):

        statusMessage = str(statusMessage).replace("'", "").replace('"', '')

        sql = ("UPDATE executions " +
               "SET " +
               "end_datetime = current_timestamp, " +
               "status = '" + status + "', " +
               "log_file = '" + 'placeholder' + "', " +
               "status_message = '" + statusMessage + "' " +
               "WHERE exec_id = " + str(execId))

        ctlDBCursor = self.datastore.cursor()
        ctlDBCursor.execute(sql)
        self.datastore.commit()

    def updateScheduleInCtlTable(self, seq, status, execId, logStr='',
                                 setStartDateTime=False,
                                 setEndDateTime=False):

        sql = "UPDATE schedules " +                                           \
              "SET " +                                                        \
              "log = '" + logStr.replace("'", "''") + "', "

        if setStartDateTime:
            sql += "start_datetime = current_timestamp, "
        if setEndDateTime:
            sql += "end_datetime = current_timestamp, "

        sql += "status = '" + status + "'"

        sql += "WHERE exec_id = " + str(execId) +                        \
               "AND   sequence = " + str(seq)

        ctlDBCursor = self.datastore.cursor()
        ctlDBCursor.execute(sql)
        self.datastore.commit()

    def getLastExec(self, ):

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

    def getScheduleFromCtlTable(self, execId):

        ctlDBCursor = self.datastore.cursor()
        ctlDBCursor.execute("SELECT exec_id, " +
                            "       sequence, " +
                            "       dataflow_name, " +
                            "       stage, " +
                            "       status, " +
                            "       start_datetime, " +
                            "       end_datetime, " +
                            "       log " +
                            "FROM   schedules " +
                            "WHERE  exec_id = " + str(execId) +
                            "ORDER BY sequence ASC")
        return ctlDBCursor.fetchall()

    def createExecutionsTable(self, ):

        ctlDBCursor = self.datastore.cursor()

        ctlDBCursor.execute("CREATE TABLE executions (" +
                            "exec_id integer primary key, " +
                            "start_datetime timestamp without " +
                            "time zone NOT NULL, " +
                            "end_datetime timestamp without time zone, " +
                            "status text NOT NULL, " +
                            "status_message text, " +
                            "bulk_or_delta text NOT NULL, " +
                            "log_file text)")

        self.datastore.commit()

    def createSchedulesTable(self, ):

        ctlDBCursor = self.datastore.cursor()

        ctlDBCursor.execute("CREATE TABLE schedules (" +
                            "exec_id integer NOT NULL, " +
                            "sequence serial NOT NULL, "
                            "dataflow_name text NOT NULL, " +
                            "stage text NOT NULL, " +
                            "status text NOT NULL, " +
                            "start_datetime timestamp without time zone, " +
                            "end_datetime timestamp without time zone, " +
                            "log text, " +
                            "PRIMARY KEY(exec_id,sequence))")

        self.datastore.commit()

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
