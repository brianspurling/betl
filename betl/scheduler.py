from . import utilities as utils
from . import conf

import traceback

log = utils.setUpLogger('SCHDLR', __name__)

# Globals
EXTRACT_FUNCTIONS = []
TRANSFORM_FUNCTIONS = []
LOAD_FUNCTIONS = []
SCHEDULE = {}


#
# Call this from your app to insert your custom functions into the schedule
#
def scheduleDataFlow(function, etlStage, pos=None):

    global EXTRACT_FUNCTIONS
    global TRANSFORM_FUNCTIONS
    global LOAD_FUNCTIONS

    if etlStage == 'EXTRACT':
        schedule = EXTRACT_FUNCTIONS
    elif etlStage == 'TRANSFORM':
        schedule = TRANSFORM_FUNCTIONS
    elif etlStage == 'LOAD':
        schedule = LOAD_FUNCTIONS
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
def executeJob(runExtract, runTransform, runLoad,
               scheduledOrManual="MANUAL"):

    global EXTRACT_FUNCTIONS
    global TRANSFORM_FUNCTIONS
    global LOAD_FUNCTIONS
    global SCHEDULE

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

    if runExtract:
        for func in EXTRACT_FUNCTIONS:
            SCHEDULE[func.__name__] = func
            sql = getJobScheduleInsertStatement(jobId, func, 'EXTRACT')
            ctlDBCursor.execute(sql)
    if runTransform:
        for func in TRANSFORM_FUNCTIONS:
            SCHEDULE[func.__name__] = func
            sql = getJobScheduleInsertStatement(jobId, func, 'TRANSFORM')
            ctlDBCursor.execute(sql)
    if runLoad:
        for func in LOAD_FUNCTIONS:
            SCHEDULE[func.__name__] = func
            sql = getJobScheduleInsertStatement(jobId, func, 'LOAD')
            ctlDBCursor.execute(sql)

    conf.CTL_DB_CONN.commit()

    # The job has now, as far as we're concerned, started, so it's crucial
    # we keep a catch-all around EVERYTHING (to ensure we update the job
    # status on-failure)

    try:

        # Pull the job_schedule back out of the control DB
        ctlDBCursor.execute("SELECT * FROM job_schedule WHERE " +
                            "job_id = " + str(jobId))
        js = ctlDBCursor.fetchall()
        for i in range(len(js)):
            logFunctionStart(js[i][0], js[i][1], js[i][2])
            logStr = executeFunction(js[i][2])
            logFunctionEnd(js[i][0], js[i][1], js[i][2], logStr)

        updateJobLog(jobId, 'SUCCESSFUL')
        print('\nBETL execution completed successfully\n\n')

    except Exception as e1:
        tb1 = traceback.format_exc()
        try:
            updateJobLog(jobId, 'FINISHED WITH ERROR', e1)
            log.critical("\n\n" +
                         "THE JOB FAILED (the job_log has been updated)\n\n" +
                         "THE error was >>> \n\n"
                         + tb1 + "\n")
            print('\nBETL execution completed with errors\n\n')

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
            print('\nBETL execution failed to complete\n\n')

    log.debug("END")


def completePreviousJob():
    # to do: firstly, check that the schedule the calling app has loaded
    # matches the schedule saved in the DB. If it doesn't, abort.
    return


#
# Update the job log
#
def updateJobLog(jobId, status, statusMessage=''):

    log.debug("START")

    ctlDBCursor = conf.CTL_DB_CONN.cursor()

    statusMessage = str(statusMessage).replace("'", "").replace('"', '')
    ctlDBCursor.execute("UPDATE job_log " +
                        "SET " +
                        "end_datetime = current_timestamp, " +
                        "status = '" + status + "', " +
                        "status_message = '" + statusMessage + "' " +
                        "WHERE job_id = " + str(jobId))
    conf.CTL_DB_CONN.commit()


#
# Check what happened last time.
#
def getStatusOfLastExecution():

    log.debug("START")

    ctlDBCursor = conf.CTL_DB_CONN.cursor()

    ctlDBCursor.execute("SELECT status FROM job_log where job_id = " +
                        "(select max(job_id) from job_log)")

    results = ctlDBCursor.fetchall()
    status = 'SUCCESSFUL'
    if len(results) > 0:  # in case it's the first execution!
        status = results[0][0]
    return status


def getJobScheduleInsertStatement(jobId, function, stage):

    sql = "INSERT " +                                                         \
          "INTO job_schedule (" +                                             \
          "job_id, " +                                                        \
          "function_name, " +                                                 \
          "stage, " +                                                         \
          "status, " +                                                        \
          "start_datetime, " +                                                \
          "end_datetime, " +                                                  \
          "log) " +                                                           \
          "VALUES (" +                                                        \
          str(jobId) + ", " +                                                 \
          "'" + function.__name__ + "', " +                                   \
          "'" + stage + "', " +                                               \
          "'PENDING', " +                                                     \
          "NULL, " +                                                          \
          "NULL, " +                                                          \
          "NULL)"
    return sql


def logFunctionStart(jobId, sequence, functionName):
    sql = "UPDATE job_schedule " +                                            \
          "SET " +                                                            \
          "status = 'RUNNING', " +                                            \
          "start_datetime = current_timestamp " +                             \
          "WHERE job_id = " + str(jobId) +                                    \
          "AND   sequence = " + str(sequence)

    ctlDBCursor = conf.CTL_DB_CONN.cursor()
    ctlDBCursor.execute(sql)
    conf.CTL_DB_CONN.commit()
    log.info("Executing " + functionName)


def executeFunction(functionName):
    global SCHEDULE
    return SCHEDULE[functionName]()


def logFunctionEnd(jobId, sequence, functionName, logStr):
    sql = "UPDATE job_schedule " +                                            \
          "SET " +                                                            \
          "status = 'SUCCESSFUL', " +                                         \
          "end_datetime = current_timestamp, " +                              \
          "log = '" + logStr.replace("'", "''") + "' " +                      \
          "WHERE job_id = " + str(jobId) +                                    \
          "AND   sequence = " + str(sequence)

    ctlDBCursor = conf.CTL_DB_CONN.cursor()
    ctlDBCursor.execute(sql)
    conf.CTL_DB_CONN.commit()
    log.info("Completed " + functionName)
