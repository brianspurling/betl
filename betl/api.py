from . import utils
from . import cli
from . import logger as logger
from .conf import Conf
from .dataIO import DataIO
from . scheduler import Scheduler
from .ctrlDB import CtrlDB

CONF = None
DATA_IO = None
JOB_LOG = None
DEV_LOG = None


def processArgs(args):
    return cli.processArgs(args)


# This function sits in api.py because it sets up the global variables
# that the other API functions rely upon
def run(appConfigFile, runTimeParams, scheduleConfig):

    global CONF
    global DATA_IO
    global JOB_LOG
    global DEV_LOG

    ###############
    # LOGGING OFF #
    ###############

    # We can't log anything until we've checked the last execution ID #

    CONF = Conf(appConfigFile, runTimeParams, scheduleConfig)
    ctrlDB = CtrlDB(CONF)

    # If we're running setup, we need to do this before checking the last
    # exec, because we're about to wipe it and start from scratch!
    # If it's successful, we log it lower down
    if CONF.exe.RUN_SETUP:
        utils.setupBetl(ctrlDB)

    # This sets the EXEC_ID in conf.state
    lastExecReport = utils.setUpExecution(CONF, ctrlDB)

    ##############
    # LOGGING ON #
    ##############

    logger.initialiseLogging(CONF.state.EXEC_ID, CONF.exe.LOG_LEVEL, CONF)
    JOB_LOG = logger.getJobLog()
    DEV_LOG = logger.getDevLog(__name__)

    JOB_LOG.info(logger.logExecutionStartFinish('START'))

    if CONF.exe.RUN_SETUP:
        JOB_LOG.info(logger.logBetlSetupComplete())
        JOB_LOG.info(logger.logExecutionOverview(lastExecReport))
    else:
        if CONF.state.RERUN_PREV_JOB:
            JOB_LOG.info(logger.logExecutionOverview(lastExecReport,
                                                     rerun=True))
        else:
            JOB_LOG.info(logger.logExecutionOverview(lastExecReport))

    # Initialise the DataIO class, that handles all DWH data input/output
    DATA_IO = DataIO(CONF)

    if CONF.exe.DELETE_TMP_DATA:
        DATA_IO.fileIO.deleteTempoaryData()

    # Pull the schema descriptions from the gsheets and our logical data models
    logicalDataModels = utils.buildLogicalDataModels(CONF)

    for dmID in logicalDataModels:
        JOB_LOG.info(logicalDataModels[dmID].__str__())

    if CONF.exe.RUN_REBUILD_ALL:
        for dataModelID in logicalDataModels:
            logicalDataModels[dataModelID].buildPhysicalDataModel()
    else:
        if CONF.exe.RUN_REBUILD_SRC:
            logicalDataModels['SRC'].buildPhysicalDataModel()
        if CONF.exe.RUN_REBUILD_STG:
            logicalDataModels['STG'].buildPhysicalDataModel()
        if CONF.exe.RUN_REBUILD_TRG:
            logicalDataModels['TRG'].buildPhysicalDataModel()
        if CONF.exe.RUN_REBUILD_SUM:
            logicalDataModels['SUM'].buildPhysicalDataModel()

    if CONF.exe.RUN_DATAFLOWS:
        scheduler = Scheduler(CONF,
                              DATA_IO,
                              logicalDataModels)
        scheduler.executeSchedule()


def readDataFromCsv(file_or_filename):
    return DATA_IO.readDataFromCsv(file_or_filename)


def readDataFromSrcSys(srcSysID, file_name_or_table_name):
    return DATA_IO.readDataFromSrcSys(
        srcSysID=srcSysID,
        file_name_or_table_name=file_name_or_table_name)


def readDataFromEtlDB(tableName):
    DATA_IO.readDataFromEtlDB(tableName=tableName)


def writeDataToCsv(df, file_or_filename, mode='w'):
    DATA_IO.writeDataToCsv(df, file_or_filename, mode)


def writeDataToTrgDB(df, tableName, if_exists):
    DATA_IO.writeDataToTrgDB(df=df,
                             tableName=tableName,
                             if_exists=if_exists)


def getSKMapping(tableName, nkColList, skColName):
    DATA_IO.getSKMapping(tableName, nkColList, skColName)


def mergeFactWithSks(df, col):
    return DATA_IO.mergeFactWithSks(df, col)


def setAuditCols(df, srcSysID, action):
    return DATA_IO.setAuditCols(df, srcSysID, action)


def logStepStart(stepDescription, stepId=None, callingFuncName=None):
    JOB_LOG.info(logger.logStepStart(stepDescription=stepDescription,
                                     stepId=stepId,
                                     callingFuncName=callingFuncName))


def logStepEnd(df):
    JOB_LOG.info(logger.logStepEnd(df=df))
