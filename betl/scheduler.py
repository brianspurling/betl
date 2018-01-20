from . import utilities as utils
from . import conf

import traceback

log = utils.setUpLogger('SCHDLR', __name__)

# Globals
EXTRACT_SCHEDULE = []
TRANSFORM_SCHEDULE = []
LOAD_SCHEDULE = []


#
# Call this from your app to insert your custom functions into the schedule
#
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
# The calling application should have scheduled 1+ functions, which we
# have stored in our global _SCHEDULE vars. Now that we're excuting the job,
# we know that the schedule is "locked down", so we're going to write it
# all to the control table.
#
# Run the ETL job, excuting each of the functions stored in the schedule
#
def executeJob(runExtract=True, runTransform=True, runLoad=True,
               scheduledOrManual="MANUAL"):

    global EXTRACT_SCHEDULE
    global TRANSFORM_SCHEDULE
    global LOAD_SCHEDULE

    log.debug("START")

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

    ctlDBCursor.execute("SELECT MAX(job_id) FROM job_log")
    jobId = ctlDBCursor.fetchall()[0][0]

    for func in EXTRACT_SCHEDULE:
        ctlDBCursor.execute("INSERT " +
                            "INTO job_schedule (" +
                            "job_id, " +
                            "function_name, " +
                            "status) " +
                            "VALUES (" +
                            str(jobId) + ", " +
                            "'" + func.__name__ + "', " +
                            "'PENDING')")
    for func in TRANSFORM_SCHEDULE:
        ctlDBCursor.execute("INSERT " +
                            "INTO job_schedule (" +
                            "job_id, " +
                            "function_name, " +
                            "status) " +
                            "VALUES (" +
                            str(jobId) + ", " +
                            "'" + func.__name__ + "', " +
                            "'PENDING')")
    for func in LOAD_SCHEDULE:
        ctlDBCursor.execute("INSERT " +
                            "INTO job_schedule (" +
                            "job_id, " +
                            "function_name, " +
                            "status) " +
                            "VALUES (" +
                            str(jobId) + ", " +
                            "'" + func.__name__ + "', " +
                            "'PENDING')")
    conf.CTL_DB_CONN.commit()

    # The job has now, as far as we're concerned, started, so it's crucial
    # we keep a catch-all around EVERYTHING (to ensure we update the job
    # status on-failure)

    try:

        if len(EXTRACT_SCHEDULE) > 0 and runExtract:
            log.info("STAGE: EXTRACT")
            for func in EXTRACT_SCHEDULE:
                log.info("Executing " + func.__name__)
                func()
                log.info("Completed " + func.__name__)
        if len(TRANSFORM_SCHEDULE) > 0 and runTransform:
            log.info("STAGE: TRANSFORM")
            for func in TRANSFORM_SCHEDULE:
                log.info("Executing " + func.__name__)
                func()
                log.info("Completed " + func.__name__)
        if len(LOAD_SCHEDULE) > 0 and runLoad:
            log.info("STAGE: LOAD")
            for func in LOAD_SCHEDULE:
                log.info("Executing " + func.__name__)
                func()
                log.info("Completed " + func.__name__)
    except Exception as e1:
        tb1 = traceback.format_exc()
        try:
            e1Str = str(e1).replace("'", "").replace('"', '')
            ctlDBCursor.execute("UPDATE job_log " +
                                "SET " +
                                "end_datetime = current_timestamp, " +
                                "status = 'FINISHED WITH ERROR', " +
                                "status_message = '" + e1Str + "' " +
                                "WHERE job_id = " + str(jobId))
            conf.CTL_DB_CONN.commit()
            log.critical("\n\n" +
                         "THE JOB FAILED (the job_log has been updated)\n\n" +
                         "THE error was >>> \n\n"
                         + tb1 + "\n")
        except Exception as e2:
            tb2 = traceback.format_exc()
            tb1 = tb1.replace("'", "")
            tb1 = tb1.replace('"', '')
            tb2 = tb2.replace("'", "")
            tb2 = tb2.replace('"', '')
            log.critical("\n\n" +
                         "THE JOB FAILED, AND THEN FAILED TO WRITE TO THE " +
                         "JOB_LOG\n\n" +
                         "THE first error was >>> \n\n"
                         + tb1 + "\n\n"
                         "The second error was >>> \n\n"
                         + tb2 + "\n")
    log.debug("END")


#
# Check what happened last time.
#
def getStatusOfLastExecution():

    log.debug("START")

    ctlDBCursor = conf.CTL_DB_CONN.cursor()

    ctlDBCursor.execute("SELECT status FROM job_log where job_id = " +
                        "(select max(job_id) from job_log)")

    results = ctlDBCursor.fetchall()
    status = 'OK'
    if len(results) > 0:
        status = results[0][0]
    return status
