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

    ctlDBCursor.execute("CREATE TABLE job_log (" +
                        "job_id serial primary key, " +
                        "start_datetime timestamp without time zone, " +
                        "end_datetime timestamp without time zone, " +
                        "status text, " +
                        "status_message text, " +
                        "bulk_or_delta text, " +
                        "scheduled_or_manual text)")
    conf.CTL_DB_CONN.commit()

    log.info("Created new control tables")
