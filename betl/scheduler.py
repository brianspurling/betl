from . import utilities as utils
from . import conf

import traceback

log = utils.setUpLogger('SCHDLR', __name__)

# Globals
EXTRACT_SCHEDULE = []
TRANSFORM_SCHEDULE = []
LOAD_SCHEDULE = []


def scheduleDataFlow(function, etlStage, pos=None):

    global EXTRACT_SCHEDULE
    global TRANSFORM_SCHEDULE
    global LOAD_SCHEDULE

    if etlStage == 'EXTRACT':
        schedule = EXTRACT_SCHEDULE
    elif etlStage == 'TRANSFORM':
        schedule = TRANSFORM_SCHEDULE
    elif etlStage == 'LOAD':
        schedule = LOAD_SCHEDULE
    else:
        raise ValueError("You can only schedule functions in one of " +
                         "the three ETL stages: EXTRACT, TRANSFORM, LOAD")

    if pos is None:
        schedule.append(function)
    else:
        schedule.insert(pos, function)


#
# Run the ETL job, excuting each of the functions stored in the schedule
#
def executeJob(runExtract=True, runTransform=True, runLoad=True,
               scheduledOrManual="MANUAL"):

    global EXTRACT_SCHEDULE
    global TRANSFORM_SCHEDULE
    global LOAD_SCHEDULE

    log.debug("START")

    # To do: should start by checking there are no uncompleted jobs
    # in the table!
    ctlDBCursor = conf.CTL_DB_CONN.cursor()

    ctlDBCursor.execute("INSERT " +
                        "INTO job_log (" +
                        "start_datetime, " +
                        "end_datetime, " +
                        "status, " +
                        "status_message, " +
                        "bulk_or_delta, " +
                        "scheduled_or_manual) " +
                        "VALUES (" +
                        "current_timestamp, " +
                        "NULL, " +
                        "'RUNNING', " +
                        "NULL, " +
                        "'" + conf.BULK_OR_DELTA + "', " +
                        "'" + scheduledOrManual + "')")

    conf.CTL_DB_CONN.commit()

    # The job has now, as far as we're concerned, started, so it's crucial
    # we keep a catch-all around EVERYTHING (to ensure we update the job
    # status on-failure)

    try:

        ctlDBCursor.execute("SELECT MAX(job_id) FROM job_log")
        jobId = ctlDBCursor.fetchall()[0][0]

        if runExtract:
            for func in EXTRACT_SCHEDULE:
                func()
        if runTransform:
            for func in TRANSFORM_SCHEDULE:
                func()
        if runLoad:
            for func in LOAD_SCHEDULE:
                func()
    except Exception as e1:
        tb1 = traceback.format_exc()
        try:
            ctlDBCursor.execute("UPDATE job_log " +
                                "SET " +
                                "end_datetime = current_timestamp, " +
                                "status = 'FINISHED WITH ERROR', " +
                                "status_message = '" + str(e1) + "'" +
                                "WHERE job_id = " + str(jobId))
            conf.CTL_DB_CONN.commit()
            log.critical("\n\n" +
                         "THE JOB FAILED (the job_log has been updated)\n\n" +
                         "THE error was >>> \n\n"
                         + tb1 + "\n")
        except Exception as e2:
            tb2 = traceback.format_exc()
            log.critical("\n\n" +
                         "THE JOB FAILED, AND THEN FAILED TO WRITE TO THE " +
                         "JOB_LOG\n\n" +
                         "THE first error was >>> \n\n"
                         + tb1 + "\n\n"
                         "The second error was >>> \n\n"
                         + tb2 + "\n")
    log.debug("END")
