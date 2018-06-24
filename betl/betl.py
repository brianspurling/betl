from . import dataflow
from . import logger
from . conf import Conf
from . import betlConfig
from . import cli
from . import reporting
from . scheduler import Scheduler


#
# BETL pipelines must call the init() method first, which configures BETL and
# sets up the Conf() object
#
# Executions are kicked off using the run() method, which executes a series of
# functions - some of these will be default functions (part of the BETL
# framework) and some will be bespoke functions (passed to init() by the
# scheduleConfig argument)
#
# Data manipulations in the bespoke functions should be performed using the
# DataFlow class. Every bespoke function is passed the instance of Betl()
# on which run() was called; this object can be used to create new instances of
# DataFlow()
#

class Betl():

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
            runTimeParams=cli.processArgs(runTimeParams),
            scheduleConfig=scheduleConfig)

        logger.initialiseLogging(self.CONF)

        ##############
        # LOGGING ON #
        ##############

        logger.logBETLStart(self.CONF)

        if self.CONF.EXE.RUN_RESET:
            # Setup would have been run as part of Conf(), but we log now
            # so that logging can be initialied first
            logger.logSetupFinish()

        if self.CONF.EXE.DELETE_TMP_DATA:
            self.CONF.EXE.deleteTempoaryData(self.CONF.CTRL.TMP_DATA_PATH)
        else:
            self.CONF.STATE.populateFileNameMap(self.CONF.CTRL.TMP_DATA_PATH)

        if self.CONF.EXE.READ_SRC:
            self.CONF.DATA.autoPopulateSrcSchemaDescriptions()

        if len(self.CONF.EXE.RUN_REBUILDS) > 0:

            logger.logRebuildPhysicalSchemaStart()

            self.CONF.DATA.refreshSchemaDescsFromGsheets()

            for dlId in self.CONF.EXE.RUN_REBUILDS:
                dlSchema = self.CONF.DATA.getDataLayerLogicalSchema(dlId)
                dlSchema.buildPhysicalSchema()

            logger.logRebuildPhysicalSchemaFinish()

    def run(self):

        response = 'SUCCESS'

        if self.CONF.EXE.RUN_DATAFLOWS:

            logger.logExecutionStart(self.CONF)

            # We need to refresh the schema descriptions first, unless we
            # did a physical schema rebuild, in which case we've done this
            # already
            if len(self.CONF.EXE.RUN_REBUILDS) == 0:
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

        if self.CONF.EXE.RUN_DATAFLOWS:

            reporting.generateExeSummary(
                conf=self.CONF,
                execId=self.CONF.STATE.EXEC_ID,
                bulkOrDelta=self.CONF.EXE.BULK_OR_DELTA,
                limitedData=self.CONF.EXE.DATA_LIMIT_ROWS)

            logger.logAlerts()

        logger.logBETLFinish(response)

        if self.CONF.EXE.RUN_DATAFLOWS:
            logger.logExecutionFinish()

    def DataFlow(self, desc):
        return dataflow.DataFlow(self.CONF, desc)


def help():
    print(cli.HELP)


def setup():
    cli.runSetup()
