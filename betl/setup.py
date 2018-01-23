from . import utilities as utils
from . import conf
import psycopg2

log = utils.setUpLogger('CONTRL', __name__)


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
        try:
            ctlDBCursor.execute("DROP TABLE " + controlTable[2])
        except psycopg2.Error as e:
            pass

    conf.CTL_DB_CONN.commit()
    log.info("Dropped " + str(counter) + ' tables')

    # Recreate the control tables

    # To do: I shouldn't have used job_id like this. A job will be a
    # predefined sequence of functions. A job_run (or something like that) is
    # what job_id below is really referring to
    ctlDBCursor.execute("CREATE TABLE job_log (" +
                        "job_id serial primary key, " +
                        "start_datetime timestamp without " +
                        "time zone NOT NULL, " +
                        "end_datetime timestamp without time zone, " +
                        "status text NOT NULL, " +
                        "status_message text, " +
                        "bulk_or_delta text NOT NULL)")

    ctlDBCursor.execute("CREATE TABLE job_schedule (" +
                        "job_id integer NOT NULL, " +
                        "sequence serial NOT NULL, "
                        "function_name text NOT NULL, " +
                        "stage text NOT NULL, " +
                        "status text NOT NULL, " +
                        "start_datetime timestamp without time zone, " +
                        "end_datetime timestamp without time zone, " +
                        "log text, " +
                        "PRIMARY KEY(job_id,sequence))")

    conf.CTL_DB_CONN.commit()

    log.info("Created new control tables")
