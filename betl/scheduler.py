from . import utilities as utils
from . import conf
from . import cli

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
        raise ValueError(cli.INVALID_STAGE_FOR_SCHEDULE)

    if pos is None:
        schedule.append(function)
    else:
        schedule.insert(pos, function)


#
# The calling application should have scheduled 1+ functions, which we
# have stored in our global _SCHEDULE vars. Now that we're excuting the job,
# and we know which stages we want to run, we're going to put the entire
# schedule into a single global var for convenience
#
def constructSchedule(runExtract, runTransform, runLoad):

    if runExtract:
        for func in EXTRACT_FUNCTIONS:
            SCHEDULE[func.__name__] = {'func': func, 'stage': 'EXTRACT'}
    if runTransform:
        for func in TRANSFORM_FUNCTIONS:
            SCHEDULE[func.__name__] = {'func': func, 'stage': 'TRANSFORM'}
    if runLoad:
        for func in LOAD_FUNCTIONS:
            SCHEDULE[func.__name__] = {'func': func, 'stage': 'LOAD'}


#
# The function that executes each dataflows in the schedule
#
def executeFunction(functionName):
    global SCHEDULE
    log.info("Executing " + functionName)
    # to do #11
    returnVal = SCHEDULE[functionName]['func']()
    log.info("Completed " + functionName)
    return returnVal


#
# Run the ETL job, excuting each of the functions stored in the schedule
#
def executeJob(jobId):

    log.debug("START")

    js = getJobSchedule(jobId)

    # to do #12
    logFile = open('logs/log_' + str(jobId) + '.txt', 'w')
    # After we update the job_log the job has started, so it's crucial
    # we keep a catch-all around EVERYTHING (to ensure we update the job
    # status on-failure)

    updateJobLog(jobId, 'RUNNING', '', logFile.name)

    try:

        for i in range(len(js)):
            # Check status of function in job_schedule (because if we are
            # re-running a failed job, we only want to pick up functions that
            # come after the point of failure

            if js[i][4] != 'SUCCESSFUL':

                updateJobSchedule(js[i][0], js[i][1], 'RUNNING', '',
                                  True, False)
                # to do #13
                lgStr = executeFunction(js[i][2])
                updateJobSchedule(js[i][0], js[i][1], 'SUCCESSFUL', lgStr,
                                  False, True)
                logFile.write(lgStr)

        updateJobLog(jobId, 'SUCCESSFUL')
        print(cli.EXECUTION_SUCCESSFUL.format(logFile=logFile))

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


###########
# Job Log #
###########

#
# Insert a new row, pending execution
#
def addJobToJobLog():
    ctlDBCursor = conf.CTL_DB_CONN.cursor()

    ctlDBCursor.execute("INSERT " +
                        "INTO job_log (" +
                        "start_datetime, " +
                        "end_datetime, " +
                        "status, " +
                        "status_message, " +
                        "bulk_or_delta) " +
                        "VALUES (" +
                        "current_timestamp, " +
                        "NULL, " +
                        "'PENDING', " +
                        "NULL, " +
                        "'" + conf.BULK_OR_DELTA + "')")

    conf.CTL_DB_CONN.commit()

    ctlDBCursor.execute("SELECT MAX(job_id) FROM job_log")

    return ctlDBCursor.fetchall()[0][0]


def logStartOfJobExecution():
    return


#
# Update the job log
#
def updateJobLog(jobId, status, statusMessage='', logFile=None):

    log.debug("START")

    ctlDBCursor = conf.CTL_DB_CONN.cursor()

    statusMessage = str(statusMessage).replace("'", "").replace('"', '')

    sql = ("UPDATE job_log " +
           "SET " +
           "end_datetime = current_timestamp, " +
           "status = '" + status + "', ")
    if logFile is not None:
        sql += ("log_file = '" + logFile + "', ")
    sql += ("status_message = '" + statusMessage + "' " +
            "WHERE job_id = " + str(jobId))
    ctlDBCursor.execute(sql)
    conf.CTL_DB_CONN.commit()


#
# Check what happened last time.
#
def getStatusOfLastExecution():

    log.debug("START")

    ctlDBCursor = conf.CTL_DB_CONN.cursor()

    ctlDBCursor.execute("SELECT job_id, status FROM job_log where job_id = " +
                        "(select max(job_id) from job_log)")

    results = ctlDBCursor.fetchall()
    status = {'jobId': None,
              'status': 'SUCCESSFUL'}
    if len(results) > 0:  # in case it's the first execution!
        status = {'jobId': results[0][0],
                  'status': results[0][1]}
    return status


################
# Job Schedule #
################
#
# We need to write the schedule to our control DB,
# So we can log progress through each dataflow, and recover from miday through
# in the case of failure
#
def writeScheduleToCntrlDb(jobId):
    for dataFlow in SCHEDULE:
        addDataflowToJobSchedule(jobId, SCHEDULE[dataFlow]['func'],
                                 SCHEDULE[dataFlow]['stage'])


#
# Insert a new row, pending execution
#
def addDataflowToJobSchedule(jobId, function, stage):
    ctlDBCursor = conf.CTL_DB_CONN.cursor()
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
    ctlDBCursor.execute(sql)
    conf.CTL_DB_CONN.commit()


#
# Pull the job_schedule back out of the control DB
#
def getJobSchedule(jobId):
    ctlDBCursor = conf.CTL_DB_CONN.cursor()
    ctlDBCursor.execute("SELECT job_id, " +
                        "       sequence, " +
                        "       function_name, " +
                        "       stage, " +
                        "       status, " +
                        "       start_datetime, " +
                        "       end_datetime, " +
                        "       log " +
                        "FROM   job_schedule " +
                        "WHERE  job_id = " + str(jobId) +
                        "ORDER BY sequence ASC")
    return ctlDBCursor.fetchall()


#
#
#
def updateJobSchedule(jobId, seq, status, logStr='',
                      setStartDateTime=False, setEndDateTime=False):
    sql = "UPDATE job_schedule " +                                            \
          "SET " +                                                            \
          "log = '" + logStr.replace("'", "''") + "', "

    if setStartDateTime:
        sql += "start_datetime = current_timestamp, "
    if setEndDateTime:
        sql += "end_datetime = current_timestamp, "

    sql += "status = '" + status + "'"

    sql += "WHERE job_id = " + str(jobId) +                                   \
           "AND   sequence = " + str(seq)

    ctlDBCursor = conf.CTL_DB_CONN.cursor()
    ctlDBCursor.execute(sql)
    conf.CTL_DB_CONN.commit()
