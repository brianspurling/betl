from . import conf
from . import logger

log = logger.setUpLogger('CONTRL', __name__)


def setupBetl():

    log.debug("START")

    ctlDBCursor = conf.CTL_DB_CONN.cursor()

    # Drop all control tables

    ctlDBCursor.execute("SELECT * " +
                        "FROM   information_schema.tables t " +
                        "WHERE  t.table_schema = 'public'")

    controlTables = ctlDBCursor.fetchall()

    counter = 0
    for controlTable in controlTables:
        counter += 1
        ctlDBCursor.execute("DROP TABLE " + controlTable[2] + " CASCADE")

    conf.CTL_DB_CONN.commit()
    log.info("Dropped " + str(counter) + ' tables')

    # Recreate the control tables

    # to do #4
    ctlDBCursor.execute("CREATE TABLE job_log (" +
                        "job_id serial primary key, " +
                        "start_datetime timestamp without " +
                        "time zone NOT NULL, " +
                        "end_datetime timestamp without time zone, " +
                        "status text NOT NULL, " +
                        "status_message text, " +
                        "bulk_or_delta text NOT NULL, " +
                        "log_file text)")

    ctlDBCursor.execute("CREATE TABLE job_schedule (" +
                        "job_id integer NOT NULL, " +
                        "sequence serial NOT NULL, "
                        "dataflow_name text NOT NULL, " +
                        "stage text NOT NULL, " +
                        "status text NOT NULL, " +
                        "start_datetime timestamp without time zone, " +
                        "end_datetime timestamp without time zone, " +
                        "log text, " +
                        "PRIMARY KEY(job_id,sequence))")

    conf.CTL_DB_CONN.commit()

    # TODO: #45 - archive old log files. see utils.deleteTempoaryData perhaps?

    log.info("Created new control tables")
