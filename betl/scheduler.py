from . import conf
from . import cli
from . import logger

import traceback

log = logger.setUpLogger('SCHDLR', __name__)

# Globals
EXTRACT_DATAFLOWS = []
TRANSFORM_DATAFLOWS = []
LOAD_DATAFLOWS = []
SCHEDULE = {}


#
# Call this from your app to insert your custom dataflows into the schedule
#
def scheduleDataFlow(dataflow, etlStage, pos=None):

    global EXTRACT_DATAFLOWS
    global TRANSFORM_DATAFLOWS
    global LOAD_DATAFLOWS

    if etlStage == 'EXTRACT':
        schedule = EXTRACT_DATAFLOWS
    elif etlStage == 'TRANSFORM':
        schedule = TRANSFORM_DATAFLOWS
    elif etlStage == 'LOAD':
        schedule = LOAD_DATAFLOWS
    else:
        raise ValueError(cli.INVALID_STAGE_FOR_SCHEDULE)

    if pos is None:
        schedule.append(dataflow)
    else:
        schedule.insert(pos, dataflow)


#
# The calling application should have scheduled 1+ dataflows, which we
# have stored in our global _SCHEDULE vars. Now that we're excuting the job,
# and we know which stages we want to run, we're going to put the entire
# schedule into a single global var for convenience
#
def constructSchedule(runExtract, runTransform, runLoad):

    if runExtract:
        for dataflow in EXTRACT_DATAFLOWS:
            SCHEDULE[dataflow.__name__] = {
                'dataflow': dataflow,
                'stage': 'EXTRACT'}
    if runTransform:
        for dataflow in TRANSFORM_DATAFLOWS:
            SCHEDULE[dataflow.__name__] = {
                'dataflow': dataflow,
                'stage': 'TRANSFORM'}
    if runLoad:
        for dataflow in LOAD_DATAFLOWS:
            SCHEDULE[dataflow.__name__] = {
                'dataflow': dataflow,
                'stage': 'LOAD'}


#
# The dataflow that executes each dataflow in the schedule
#
def executeFunction(dataflowName):
    global SCHEDULE
    log.debug("Executing " + dataflowName)

    # We set the conf.STAGE object so that, during execution of the dataflow,
    # we know which stage we're in
    conf.STAGE = SCHEDULE[dataflowName]['stage']
    SCHEDULE[dataflowName]['dataflow']()  # to do #11
    log.debug("Completed " + dataflowName)


#
# Run the ETL job, excuting each of the dataflows stored in the schedule
#
def executeJob(jobId):

    log.debug("START")

    js = getJobSchedule(jobId)

    logger.createJobLogFile(jobId)

    # After we update the job_log the job has started, so it's crucial
    # we keep a catch-all around EVERYTHING (to ensure we update the job
    # status on-failure)

    updateJobLog(jobId, 'RUNNING', '')

    try:

        for i in range(len(js)):
            # Check status of dataflow in job_schedule (because if we are
            # re-running a failed job, we only want to pick up dataflows that
            # come after the point of failure

            if js[i][4] != 'SUCCESSFUL':
                updateJobSchedule(
                    jobId=js[i][0],
                    seq=js[i][1],
                    status='RUNNING',
                    logStr='',
                    setStartDateTime=True,
                    setEndDateTime=False)

                ########################
                # EXECUTE THE DATAFLOW #
                ########################

                executeFunction(js[i][2])  # to do #13

                #########################
                #########################
                #########################

                updateJobSchedule(
                    jobId=js[i][0],
                    seq=js[i][1],
                    status='SUCCESSFUL',
                    logStr='',
                    setStartDateTime=False,
                    setEndDateTime=True)

        updateJobLog(jobId, 'SUCCESSFUL', '')
        logStr = ("\n\n" +
                  "THE JOB COMPLETED SUCCESSFULLY " +
                  "(the job_log has been updated)\n\n")
        logger.appendLogToFile(logStr)

        print(cli.EXECUTION_SUCCESSFUL.format(logFile=logger.getLogFileName()))

    except Exception as e1:
        tb1 = traceback.format_exc()
        try:
            updateJobSchedule(
                jobId=js[i][0],
                seq=js[i][1],
                status='FINISHED WITH ERROR',
                logStr=tb1,
                setStartDateTime=False,
                setEndDateTime=True)
            updateJobLog(jobId, 'FINISHED WITH ERROR', tb1)
            logStr = ("\n\n" +
                      "THE JOB FAILED (the job_log has been updated)\n\n" +
                      "THE error was >>> \n\n"
                      + tb1 + "\n")
            log.critical(logStr)
            logger.appendLogToFile(logStr)  # Need to test output
            print('\nBETL execution completed with errors\n\n')

        except Exception as e2:
            tb2 = traceback.format_exc()
            tb1 = tb1.replace("'", "")
            tb1 = tb1.replace('"', '')
            tb2 = tb2.replace("'", "")
            tb2 = tb2.replace('"', '')
            logStr = ("\n\n" +
                      "THE JOB FAILED, AND THEN FAILED TO WRITE TO THE " +
                      "JOB_LOG\n\n" +
                      "THE first error was >>> \n\n"
                      + tb1 + "\n\n"
                      "The second error was >>> \n\n"
                      + tb2 + "\n")
            log.critical(logStr)
            logger.appendLogToFile(logStr)

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
        addDataflowToJobSchedule(jobId, SCHEDULE[dataFlow]['dataflow'],
                                 SCHEDULE[dataFlow]['stage'])


#
# Insert a new row, pending execution
#
def addDataflowToJobSchedule(jobId, dataflow, stage):
    ctlDBCursor = conf.CTL_DB_CONN.cursor()
    sql = "INSERT " +                                                         \
          "INTO job_schedule (" +                                             \
          "job_id, " +                                                        \
          "dataflow_name, " +                                                 \
          "stage, " +                                                         \
          "status, " +                                                        \
          "start_datetime, " +                                                \
          "end_datetime, " +                                                  \
          "log) " +                                                           \
          "VALUES (" +                                                        \
          str(jobId) + ", " +                                                 \
          "'" + dataflow.__name__ + "', " +                                   \
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
                        "       dataflow_name, " +
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


#
# Update the job log
#
def updateJobLog(jobId, status, statusMessage=''):

    log.debug("START")

    ctlDBCursor = conf.CTL_DB_CONN.cursor()

    statusMessage = str(statusMessage).replace("'", "").replace('"', '')

    sql = ("UPDATE job_log " +
           "SET " +
           "end_datetime = current_timestamp, " +
           "status = '" + status + "', " +
           "log_file = '" + logger.getLogFileName() + "', " +
           "status_message = '" + statusMessage + "' " +
           "WHERE job_id = " + str(jobId))
    ctlDBCursor.execute(sql)
    conf.CTL_DB_CONN.commit()
