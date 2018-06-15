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

        logger.logExecutionStart(self.CONF)

        if self.CONF.EXE.DELETE_TMP_DATA:
            self.CONF.EXE.deleteTempoaryData(self.CONF.CTRL.TMP_DATA_PATH)
        else:
            self.CONF.STATE.populateFileNameMap(self.CONF.CTRL.TMP_DATA_PATH)

        if self.CONF.EXE.READ_SRC:
            self.CONF.DATA.autoPopulateSrcSchemaDescriptions()

        self.CONF.DATA.refreshSchemaDescsFromGsheets()

        if len(self.CONF.EXE.RUN_REBUILDS) > 0:
            logger.logPhysicalDataModelBuild()
            for dataLayer in self.CONF.EXE.RUN_REBUILDS:
                logDataModel = self.CONF.DATA.getLogicalDataModel(dataLayer)
                logDataModel.buildPhysicalDataModel()

    def run(self):

        if self.CONF.EXE.RUN_DATAFLOWS:
            # This is the main execution of the data pipeline
            response = Scheduler(self.CONF).execute(self)
        else:
            response = 'SUCCESS'

        if response == 'SUCCESS':

            self.CONF.DATA.checkDBsForSuperflousTables()

            self.CONF.CTRL.CTRL_DB.updateExecution(
                execId=self.CONF.STATE.EXEC_ID,
                status='SUCCESSFUL',
                statusMessage='')

            logStr = ("\n" +
                      "THE JOB COMPLETED SUCCESSFULLY " +
                      "(the executions table has been updated)\n\n")

            reporting.generateExeSummary(
                conf=self.CONF,
                execId=self.CONF.STATE.EXEC_ID,
                bulkOrDelta=self.CONF.EXE.BULK_OR_DELTA,
                limitedData=self.CONF.EXE.DATA_LIMIT_ROWS)

            logger.logExecutionFinish(logStr)

    def DataFlow(self, desc):
        return dataflow.DataFlow(self.CONF, desc)
