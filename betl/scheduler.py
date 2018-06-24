import traceback
from . import logger
from . import alerts
from . import df_extract
from . import df_transform
from . import df_load
from . import df_summarise
from . import df_dmDate
from . import df_dmAudit


class Scheduler():

    def __init__(self, conf):

        self.jobLog = logger.getLogger()
        self.conf = conf

        self.funcSequence = 0
        self.functions_list = []
        self.functions_dict = {}

        # We must construct the scheduler (even if we're re-running the prev
        # execution). buildFunctionList puts all the actual function objects
        # into both the list and dict attributes.
        self.buildFunctionList()

        # However, we only write a new set of functions to the ctrlDB if
        # this is a new (not rerun) execution
        if not self.conf.STATE.RERUN_PREV_JOB:
            self.conf.CTRL.CTRL_DB.insertFunctions(
                self.functions_dict,
                conf.STATE.EXEC_ID)

    def buildFunctionList(self):

        if self.conf.EXE.RUN_EXTRACT:
            if self.conf.SCHEDULE.DEFAULT_EXTRACT:
                self.addFunctionToList(
                    function=df_extract.defaultExtract,
                    stage='EXTRACT')

                self.srcTablesToExcludeFromExtract = \
                    self.conf.SCHEDULE.SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT

            for function in self.conf.SCHEDULE.EXTRACT_DFS:
                self.addFunctionToList(
                    function=function,
                    stage='EXTRACT')

        if self.conf.EXE.RUN_TRANSFORM:

            for function in self.conf.SCHEDULE.TRANSFORM_DFS:
                self.addFunctionToList(
                    function=function,
                    stage='TRANSFORM')

            if self.conf.SCHEDULE.DEFAULT_DM_DATE:
                self.addFunctionToList(
                    function=df_dmDate.transformDMDate,
                    stage='TRANSFORM')

            if self.conf.SCHEDULE.DEFAULT_DM_AUDIT:
                self.addFunctionToList(
                    function=df_dmAudit.transformDMAudit,
                    stage='TRANSFORM')

            if self.conf.SCHEDULE.DEFAULT_TRANSFORM:
                self.addFunctionToList(
                    function=df_transform.defaultTransform,
                    stage='TRANSFORM')

        if self.conf.EXE.RUN_LOAD:

            if self.conf.SCHEDULE.DEFAULT_LOAD:
                self.addFunctionToList(
                    function=df_load.defaultLoad,
                    stage='LOAD')

            for function in self.conf.SCHEDULE.LOAD_DFS:
                self.addFunctionToList(
                    function=function,
                    stage='LOAD')

        if self.conf.EXE.RUN_SUMMARISE:

            if self.conf.SCHEDULE.DEFAULT_SUMMARISE:
                self.addFunctionToList(
                    function=df_summarise.defaultSummarisePrep,
                    stage='SUMMARISE')

            for function in self.conf.SCHEDULE.SUMMARISE_DFS:
                self.addFunctionToList(
                    function=function,
                    stage='SUMMARISE')

            if self.conf.SCHEDULE.DEFAULT_SUMMARISE:
                self.addFunctionToList(
                    function=df_summarise.defaultSummariseFinish,
                    stage='SUMMARISE')

    def addFunctionToList(self, function, stage):
        self.funcSequence += 1
        self.functions_list.append({
            'function': function,
            'stage': stage,
            'sequence': self.funcSequence})
        self.functions_dict[function.__name__] = {
            'function': function,
            'stage': stage,
            'sequence': self.funcSequence}

    def execute(self, betl):

        functions = self.conf.CTRL.CTRL_DB.getFunctionsForExec(
            execId=self.conf.STATE.EXEC_ID)

        self.conf.CTRL.CTRL_DB.updateExecution(
            execId=self.conf.STATE.EXEC_ID,
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
                    self.conf.CTRL.CTRL_DB.updateFunction(
                        execId=self.conf.STATE.EXEC_ID,
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

                    self.conf.CTRL.CTRL_DB.updateFunction(
                        execId=self.conf.STATE.EXEC_ID,
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
        # We set the conf.STATE.STAGE object so that, during execution of the
        # function,  we know which stage we're in
        self.conf.STATE.setStage(self.functions_dict[functionName]['stage'])
        self.conf.STATE.setFunctionId(functionId)
        self.functions_dict[functionName]['function'](betl)

    def handleFunctionException(self, functions, counter, errorMessage):
            tb1 = traceback.format_exc()
            try:
                self.conf.CTRL.CTRL_DB.updateFunction(
                    execId=self.conf.STATE.EXEC_ID,
                    functionName=functions[counter][2],
                    status='FINISHED WITH ERROR',
                    logStr=tb1,
                    setStartDateTime=False,
                    setEndDateTime=True)
                self.conf.CTRL.CTRL_DB.updateExecution(
                    execId=self.conf.STATE.EXEC_ID,
                    status='FINISHED WITH ERROR',
                    statusMessage=tb1
                )

                alert = ("THE JOB FAILED (the executions table has been " +
                         "updated)\n\n" +
                         "THE error was >>> \n\n" + tb1)
                alerts.logAlert(self.conf, alert)
                logger.logBETLFinish('FAILED')
                logger.logExecutionFinish()

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
                logger.logBETLFinish('FAILED_RECOVERY')
                logger.logExecutionFinish()
