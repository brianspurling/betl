import traceback
from betl.logger import Logger
from betl.logger import alerts

from betl.defaultdataflows import stageExtract
from betl.defaultdataflows import stageLoad
from betl.defaultdataflows import stageSummarise
from betl.defaultdataflows import dmDate
from betl.defaultdataflows import dmAudit


    # TODO: this code is no longer being used. When I migrated to
    # Airflow I dumped this, as I wasn't sure how it would
    # overlap with Airflow functionality. Pipeline now creates
    # operators directly. Some of the code below is now clearly
    # obsolete, e.g. the actual execution of functions, but there's
    # some logging that's been lost in the process, which I think
    # I will eventually want back, and the control DB is no longer
    # being used (removing audit as well as ability to re-run from
    # failure). I've also left if response == 'SUCCESS': self.CONF.checkDBsForSuperflousTables(self.CONF) out of the
    # execution


class Scheduler():

    def __init__(self, betl):



        # However, we only write a new set of functions to the ctrlDB if
        # this is a new (not rerun) execution
        if not self.CONF.RERUN_PREV_JOB:
            self.CONF.CTRL_DB.insertFunctions(
                self.functions_dict,
                CONF.EXEC_ID)

        # If we are rerunning a prev job, we need to delete the failed
        # dataflow record from the control table (because we'll be inserting
        # a new one when the dataflow re-runs, but we can't have dupes)
        if self.CONF.RERUN_PREV_JOB:
            self.CONF.CTRL_DB.deleteFailedDataflow(CONF.EXEC_ID)
            alert = "JOB RESTARTED"
            alerts.logAlert(self.conf, alert)





    def execute(self, betl):

        functions = self.CONF.CTRL_DB.getFunctionsForExec(
            execId=self.CONF.EXEC_ID)

        self.CONF.CTRL_DB.updateExecution(
            execId=self.CONF.EXEC_ID,
            status='RUNNING',
            statusMessage='')

        counter = 0  # Keeping track of the loop iterator for the catch-all
        try:
            for i in range(len(functions)):
                counter = i
                # Check status of function in ctrlDB.funtions (because if we
                # are re-running a failed job, we only want to pick up
                # funtions that come after the point of failure

                if functions[i][5] != 'SUCCESSFUL':
                    self.CONF.CTRL_DB.updateFunction(
                        execId=self.CONF.EXEC_ID,
                        functionName=functions[i][2],
                        status='RUNNING',
                        logStr='',
                        setStartDateTime=True,
                        setEndDateTime=False)

                    ########################
                    # EXECUTE THE FUNCTION #
                    ########################

                    self.executeFunction(
                        betl=betl,
                        functionName=functions[i][2],
                        functionId=functions[i][0])

                    #########################
                    #########################
                    #########################

                    self.CONF.CTRL_DB.updateFunction(
                        execId=self.CONF.EXEC_ID,
                        functionName=functions[i][2],
                        status='SUCCESSFUL',
                        logStr='',
                        setStartDateTime=False,
                        setEndDateTime=True)

            return 'SUCCESS'

        # Catch everything, so we can output to the logs
        except Exception as e1:
            self.handleFunctionException(functions, counter, e1)
            return 'FAIL'

    def executeFunction(self, betl, functionName, functionId):
        # We set the CONF.STAGE object so that, during execution of the
        # function,  we know which stage we're in
        self.CONF.setStage(self.functions_dict[functionName]['stage'])
        self.CONF.setFunctionId(functionId)
        self.functions_dict[functionName]['function'](betl)

    def handleFunctionException(self, functions, counter, errorMessage):
            tb1 = traceback.format_exc()
            try:

                # Rollback first, to close down any existing (failed) trans
                self.CONF.CTRL_DB.datastore.rollback()

                self.CONF.CTRL_DB.updateFunction(
                    execId=self.CONF.EXEC_ID,
                    functionName=functions[counter][2],
                    status='FINISHED WITH ERROR',
                    logStr=tb1,
                    setStartDateTime=False,
                    setEndDateTime=True)
                self.CONF.CTRL_DB.updateExecution(
                    execId=self.CONF.EXEC_ID,
                    status='FINISHED WITH ERROR',
                    statusMessage=tb1
                )

                alert = ("THE JOB FAILED (the executions table has been " +
                         "updated)\n\n" +
                         "THE error was >>> \n\n" + tb1)
                alerts.logAlert(self.conf, alert)
                self.log.logBETLFinish('FAILED')
                self.log.logExecutionFinish()

            except Exception as e2:
                tb2 = traceback.format_exc()
                # TODO: why?
                tb1 = tb1.replace("'", "")
                tb1 = tb1.replace('"', '')
                tb2 = tb2.replace("'", "")
                tb2 = tb2.replace('"', '')

                alert = ("THE JOB FAILED, AND THEN FAILED TO WRITE TO THE " +
                         "JOB_LOG\n\n" +
                         "THE first error was >>> \n\n" + tb1 + "\n\n" +
                         "The second error was >>> \n\n" + tb2)
                alerts.logAlert(self.conf, alert)
                self.log.logBETLFinish('FAILED_RECOVERY')
                self.log.logExecutionFinish()
