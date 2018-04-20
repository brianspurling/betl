import traceback
from . import logger as logger
from .ctrlDB import CtrlDB
from . import df_extract
from . import df_dmDate
from . import df_load
from . import df_summarise


class Scheduler():

    def __init__(self, conf, dataIO, logicalDataModels):

        self.devLog = logger.getDevLog(__name__)
        self.jobLog = logger.getJobLog()

        self.logicalDataModels = logicalDataModels
        self.scheduleList = []
        self.scheduleDic = {}
        self.srcTablesToExcludeFromExtract = []
        self.trgTablesToExcludeFromLoad = []
        self.bulkOrDelta = conf.exe.BULK_OR_DELTA

        self.conf = conf
        self.dataIO = dataIO

        self.ctrlDB = CtrlDB(conf)

        # We must construct the scheduler even if we're re-running the prev
        # load. constructSchedule puts all the actual funcs into the dict,
        # so that when we pull the func names out of the ctrlDB we can "find"
        # the actual func to run.
        self.constructSchedule()

        if not self.conf.state.RERUN_PREV_JOB:
            self.ctrlDB.insertNewScheduleToCtlTable(self.scheduleDic,
                                                    conf.state.EXEC_ID)

    def constructSchedule(self):

        schedule = self.conf.schedule
        if self.conf.exe.RUN_EXTRACT:
            if schedule.DEFAULT_EXTRACT:
                self.scheduleDataflow(df_extract.defaultExtract, 'EXTRACT')

                self.srcTablesToExcludeFromExtract = \
                    schedule.SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT

            for dataflow in schedule.EXTRACT_DFS:
                self.scheduleDataflow(dataflow, 'EXTRACT')

        if self.conf.exe.RUN_TRANSFORM:

            if schedule.DEFAULT_DM_DATE:
                self.scheduleDataflow(df_dmDate.transformDMDate, 'TRANSFORM')

            for dataflow in schedule.TRANSFORM_DFS:
                self.scheduleDataflow(dataflow, 'TRANSFORM')

        if self.conf.exe.RUN_LOAD:

            if schedule.DEFAULT_LOAD:
                self.scheduleDataflow(df_load.defaultLoad, 'LOAD')
                self.trgTablesToExcludeFromLoad = \
                    schedule.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

            for dataflow in schedule.LOAD_DFS:
                self.scheduleDataflow(dataflow, 'LOAD')

        if self.conf.exe.RUN_SUMMARISE:

            if schedule.DEFAULT_SUMMARISE:
                self.scheduleDataflow(
                    df_summarise.defaultSummarise,
                    'SUMMARISE')

            for dataflow in schedule.SUMMARISE_DFS:
                self.scheduleDataflow(dataflow, 'SUMMARISE')

    def scheduleDataflow(self, dataflow, stage):
        self.scheduleList.append({
            'dataflow': dataflow,
            'stage': stage})
        self.scheduleDic[dataflow.__name__] = {
            'dataflow': dataflow,
            'stage': stage}

    def executeSchedule(self):

        schedule = self.ctrlDB.getScheduleFromCtlTable(self.conf.state.EXEC_ID)

        self.ctrlDB.updateExecutionInCtlTable(execId=self.conf.state.EXEC_ID,
                                              status='RUNNING',
                                              statusMessage='')
        counter = 0  # Keeping track of the loop iterator the catch-all
        try:
            for i in range(len(schedule)):
                counter = i
                # Check status of dataflow in schedules (because if we are
                # re-running a failed job, we only want to pick up dataflows
                # that come after the point of failure

                if schedule[i][4] != 'SUCCESSFUL':
                    self.ctrlDB.updateScheduleInCtlTable(
                        seq=schedule[i][1],
                        status='RUNNING',
                        execId=self.conf.state.EXEC_ID,
                        logStr='',
                        setStartDateTime=True,
                        setEndDateTime=False)

                    ########################
                    # EXECUTE THE DATAFLOW #
                    ########################

                    self.executeDataflow(schedule[i][2])  # to do #13

                    #########################
                    #########################
                    #########################

                    self.ctrlDB.updateScheduleInCtlTable(
                        seq=schedule[i][1],
                        status='SUCCESSFUL',
                        execId=self.conf.state.EXEC_ID,
                        logStr='',
                        setStartDateTime=False,
                        setEndDateTime=True)

            return 'SUCCESS'

        # Catch everything, so we can output to the logs
        except Exception as e1:
            self.handleDataflowException(schedule, counter, e1)
            return 'FAIL'

    def executeDataflow(self, dataflowName):
        # We set the conf.STAGE object so that, during execution of the
        # dataflow  we know which stage we're in
        self.conf.state.setStage(self.scheduleDic[dataflowName]['stage'])
        self.devLog.info('Starting execution of dataflow: ' + dataflowName)
        self.scheduleDic[dataflowName]['dataflow'](self)
        self.devLog.info('Completed execution of dataflow: ' + dataflowName)

    def handleDataflowException(self, schedule, counter, errorMessage):
            tb1 = traceback.format_exc()
            try:
                self.ctrlDB.updateScheduleInCtlTable(
                    seq=schedule[counter][1],
                    status='FINISHED WITH ERROR',
                    execId=self.conf.state.EXEC_ID,
                    logStr=tb1,
                    setStartDateTime=False,
                    setEndDateTime=True)
                self.ctrlDB.updateExecutionInCtlTable(
                    execId=self.conf.state.EXEC_ID,
                    status='FINISHED WITH ERROR',
                    statusMessage=tb1
                )
                logStr = ("\n\n" +
                          "THE JOB FAILED (the executions table has been " +
                          "updated)\n\n" +
                          "THE error was >>> \n\n"
                          + tb1 + "\n")
                self.jobLog.critical(logStr)
                self.jobLog.info(logger.logExecutionStartFinish('FINISH'))

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
                logStr += ''
                self.devLog.critical(logStr)
                self.jobLog.info(logger.logExecutionStartFinish('FINISH'))
