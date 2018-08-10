from .dataflow import DataFlow
from .logger import Logger
from .conf import Conf
from . import betlConfig
from .conf import processArgs
from .reporting import reporting
from .scheduler import Scheduler


#
# A new pipeline's init() method configures BETL and sets up the Conf() object
#
# Executions are kicked off using the run() method, which executes a series of
# functions - some of these will be default functions (part of the BETL
# framework) and some will be bespoke functions (passed to init() by the
# scheduleConfig argument)
#
# Data manipulations in the bespoke functions should be performed using the
# DataFlow class. Every bespoke function is passed the instance of
# betl.pipeline() on which run() was called; this object can be used to create
# new instances of DataFlow()
#

class Pipeline():

    def __init__(self, appConfigFile, runTimeParams, scheduleConfig=None):

        # We can't initialise our logger until we have an execID, which is
        # assigned in the Conf() constructor

        ###############
        # LOGGING OFF #
        ###############

        if scheduleConfig is None:
            scheduleConfig = betlConfig.defaultScheduleConfig

        self.CONF = Conf(
            appConfigFile=appConfigFile,
            runTimeParams=processArgs(runTimeParams),
            scheduleConfig=scheduleConfig)

        Logger.initialiseLogging(
            execId=self.CONF.STATE.EXEC_ID,
            logLevel=self.CONF.EXE.LOG_LEVEL,
            logPath=self.CONF.CTRL.LOG_PATH,
            auditCols=self.CONF.DATA.AUDIT_COLS)

        self.log = Logger()

        # TODO: change logger's initiliaiseLogging so that it sets up a file
        # without yet knowing the exec_id, then put
        # LoggerClass.initiliaiseLogging at the top, and create the Logger
        # instance immediately afterwards. Then call setExecId after Conf()

        ##############
        # LOGGING ON #
        ##############

        self.log.logBETLStart(self.CONF.STATE.RERUN_PREV_JOB)

        if self.CONF.EXE.RUN_RESET:
            # Setup would have been run as part of Conf(), but we log now
            # so that logging can be initialied first
            self.log.logSetupFinish()

        if self.CONF.EXE.DELETE_TMP_DATA:
            self.CONF.EXE.deleteTempoaryData(self.CONF.CTRL.TMP_DATA_PATH)
        else:
            self.CONF.STATE.populateFileNameMap(self.CONF.CTRL.TMP_DATA_PATH)

        if self.CONF.EXE.READ_SRC:
            self.CONF.DATA.autoPopulateSrcSchemaDescriptions()

        if len(self.CONF.EXE.RUN_REBUILDS) > 0:

            self.log.logRebuildPhysicalSchemaStart()

            self.CONF.DATA.refreshSchemaDescsFromGsheets()

            for dlId in self.CONF.EXE.RUN_REBUILDS:
                dlSchema = self.CONF.DATA.getDataLayerLogicalSchema(dlId)
                dlSchema.buildPhysicalSchema()

            self.log.logRebuildPhysicalSchemaFinish()

    def run(self):

        response = 'SUCCESS'

        if self.CONF.EXE.RUN_DATAFLOWS:

            self.log.logExecutionStart(
                rerunPrevJob=self.CONF.STATE.RERUN_PREV_JOB,
                lastExecReport=self.CONF.LAST_EXEC_REPORT)

            # If we need to refresh the schema descriptions, we pull them
            # from Gsheets and update our quick-access txt files. Unless we
            # did a physical schema rebuild, in which case we've done this
            # already
            if self.CONF.EXE.REFRESH_SCHEMA \
               and len(self.CONF.EXE.RUN_REBUILDS) == 0:
                self.CONF.DATA.refreshSchemaDescsFromGsheets()

            # This is the main execution of the data pipeline
            response = Scheduler(self.CONF).execute(self)

            # Don't check for superflous tables on failure, because if the
            # failure was a DB failure we won't be able to access the DB
            if response == 'SUCCESS':
                self.CONF.DATA.checkDBsForSuperflousTables(self.CONF)

        # Even if we didn't execute the dataflows, we still created a new
        # execution in the CtrlDB, so we need to mark this as complete
        if response == 'SUCCESS':

            self.CONF.CTRL.CTRL_DB.updateExecution(
                execId=self.CONF.STATE.EXEC_ID,
                status='SUCCESSFUL',
                statusMessage='')

        if self.CONF.EXE.RUN_DATAFLOWS and response == 'SUCCESS':

            reporting.generateExeSummary(
                conf=self.CONF,
                execId=self.CONF.STATE.EXEC_ID,
                bulkOrDelta=self.CONF.EXE.BULK_OR_DELTA,
                limitedData=self.CONF.EXE.DATA_LIMIT_ROWS)

        if self.CONF.EXE.RUN_DATAFLOWS:

            self.log.logAlerts()

        self.log.logBETLFinish(response)

        if self.CONF.EXE.RUN_DATAFLOWS:
            self.log.logExecutionFinish()

    def DataFlow(self, desc):
        return DataFlow(desc=desc, conf=self.CONF)
