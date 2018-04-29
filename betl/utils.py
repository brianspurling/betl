import sys
import shutil
import os
import datetime
import time

from . import cli
from . import logger
from . import fileIO
from .dataLayer import SrcDataLayer
from .dataLayer import StgDataLayer
from .dataLayer import TrgDataLayer
from .dataLayer import SumDataLayer
from .conf import Conf
from .scheduler import Scheduler

JOB_LOG = None
DEV_LOG = None


def init(appConfigFile, runTimeParams, scheduleConfig=None):
    global JOB_LOG
    global DEV_LOG

    ###############
    # LOGGING OFF #
    ###############

    if scheduleConfig is None:
        scheduleConfig = getDetaulfScheduleConfig()

    # We can't log anything until we've checked the last execution ID #
    conf = Conf(appConfigFile, cli.processArgs(runTimeParams), scheduleConfig)

    # If we're running setup, we need to do this before checking the last
    # exec, because we're about to wipe it and start from scratch!
    # If it's successful, we log it lower down
    if conf.exe.RUN_SETUP:
        setupBetl(conf)

    # This sets the EXEC_ID in conf.state
    lastExecReport = setUpExecution(conf)

    ##############
    # LOGGING ON #
    ##############

    logger.initialiseLogging(conf)
    JOB_LOG = logger.getLogger()
    DEV_LOG = logger.getDevLog(__name__)

    JOB_LOG.info(logger.logExecutionStartFinish(
        'START',
        rerun=conf.state.RERUN_PREV_JOB))

    if conf.exe.RUN_SETUP:
        JOB_LOG.info(logger.logBetlSetupComplete())
        JOB_LOG.info(logger.logExecutionOverview(lastExecReport))
    else:
        if conf.state.RERUN_PREV_JOB:
            JOB_LOG.info(logger.logExecutionOverview(lastExecReport,
                                                     rerun=True))
        else:
            JOB_LOG.info(logger.logExecutionOverview(lastExecReport))

    if conf.exe.DELETE_TMP_DATA:
        fileIO.deleteTempoaryData(conf.app.TMP_DATA_PATH)
    else:
        conf.state.populateFileNameMap(conf.app.TMP_DATA_PATH)

    # Pull the schema descriptions from the gsheets and our logical data models
    logicalDataModels = buildLogicalDataModels(conf)
    conf.state.setLogicalDataModels(logicalDataModels)

    for dmID in logicalDataModels:
        JOB_LOG.info(logicalDataModels[dmID].__str__())

    if conf.exe.RUN_REBUILD_ALL or \
       conf.exe.RUN_REBUILD_SRC or \
       conf.exe.RUN_REBUILD_STG or \
       conf.exe.RUN_REBUILD_TRG or \
       conf.exe.RUN_REBUILD_SUM:
        JOB_LOG.info(logger.logPhysicalDataModelBuildStart())

    if conf.exe.RUN_REBUILD_ALL:
        for dataModelID in logicalDataModels:
            logicalDataModels[dataModelID].buildPhysicalDataModel()
    else:
        if conf.exe.RUN_REBUILD_SRC:
            logicalDataModels['SRC'].buildPhysicalDataModel()
        if conf.exe.RUN_REBUILD_STG:
            logicalDataModels['STG'].buildPhysicalDataModel()
        if conf.exe.RUN_REBUILD_TRG:
            logicalDataModels['TRG'].buildPhysicalDataModel()
        if conf.exe.RUN_REBUILD_SUM:
            logicalDataModels['SUM'].buildPhysicalDataModel()

    checkDBsForSuperflousTables(conf)
    return conf


def run(conf):

    response = 'SUCCESS'

    if conf.exe.RUN_DATAFLOWS:
        scheduler = Scheduler(conf)
        response = scheduler.executeSchedule()

    if response == 'SUCCESS':
        conf.app.CTRL_DB.updateExecutionInCtlTable(
            execId=conf.state.EXEC_ID,
            status='SUCCESSFUL',
            statusMessage='')
        logStr = ("\n\n" +
                  "THE JOB COMPLETED SUCCESSFULLY " +
                  "(the executions table has been updated)\n\n")
        JOB_LOG.info(logStr)
        JOB_LOG.info(logger.logExecutionStartFinish('FINISH'))


def getDetaulfScheduleConfig():
    scheduleConfig = {
        'DEFAULT_EXTRACT': False,
        'SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT': [],
        'DEFAULT_LOAD': False,
        'DEFAULT_SUMMARISE': False,
        'DEFAULT_DM_DATE': False,
        'TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],
        'EXTRACT_DFS': [],
        'TRANSFORM_DFS': [],
        'LOAD_DFS': [],
        'SUMMARISE_DFS': []
    }

    return scheduleConfig


def setUpExecution(conf):

    # Log in to the CTL DB and check the status of the last run
    lastExecDetails = getDetailsOfLastExecution(conf.app.CTRL_DB)
    lastExecStatus = lastExecDetails['lastExecStatus']
    lastExecId = lastExecDetails['lastExecId']

    execId = None

    if lastExecStatus == 'NO_PREV_EXEC':
        execId = 1
    elif (lastExecStatus == 'RUNNING' and
          conf.exe.RUN_DATAFLOWS and
          not conf.exe.SKIP_WARNINGS):

        text = input(cli.LAST_EXE_STILL_RUNNING)
        sys.exit()

    elif (lastExecStatus != 'SUCCESSFUL' and
          conf.exe.RUN_DATAFLOWS and
          not conf.exe.SKIP_WARNINGS):

        text = input(cli.LAST_EXE_FAILED.format(status=lastExecStatus))
        if text.lower() != 'ignore':
            if conf.exe.RUN_SETUP or \
               conf.exe.RUN_REBUILD_ALL or \
               conf.exe.RUN_REBUILD_SRC or \
               conf.exe.RUN_REBUILD_STG or \
               conf.exe.RUN_REBUILD_TRG or \
               conf.exe.RUN_REBUILD_SUM:
                text = input(cli.CANT_RERUN_WITH_SETUP_OR_REBUILD)
                sys.exit()
            else:
                conf.state.RERUN_PREV_JOB = True
                execId = lastExecId
        else:
            execId = lastExecId + 1
    else:
        execId = lastExecId + 1

    conf.state.setExecID(execId)

    if not conf.state.RERUN_PREV_JOB:
        conf.app.CTRL_DB.insertNewExecutionToCtlTable(
            execId,
            conf.exe.BULK_OR_DELTA)

    lastExecReport = {
        'lastExecId': lastExecId,
        'lastExecStatus': lastExecStatus,
        'execId': execId
    }
    return lastExecReport


def getDetailsOfLastExecution(ctlDB):

    lastExecRow = ctlDB.getLastExec()

    lastExecDetails = {}
    if len(lastExecRow) > 0:  # in case it's the first execution!
        lastExecDetails = {'lastExecId': lastExecRow[0][0],
                           'lastExecStatus': lastExecRow[0][1]}
    else:
        lastExecDetails = {'lastExecId': None,
                           'lastExecStatus': 'NO_PREV_EXEC'}
    return lastExecDetails


def setupBetl(conf):
    conf.app.CTRL_DB.dropAllCtlTables()
    archiveLogFiles(conf)
    conf.app.CTRL_DB.createExecutionsTable()
    conf.app.CTRL_DB.createSchedulesTable()


def archiveLogFiles(conf):

    timestamp = datetime.datetime.fromtimestamp(
        time.time()
    ).strftime('%Y%m%d%H%M%S')

    source = conf.app.LOG_PATH
    dest = conf.app.LOG_PATH + 'archive_' + timestamp + '/'

    if not os.path.exists(dest):
        os.makedirs(dest)

    files = os.listdir(source)
    for f in files:
        if f.find('.log') > -1:
            shutil.move(source+f, dest)


def buildLogicalDataModels(conf):

    logicalDataModels = {}

    print('\n', end='')
    print('*** Building the logical data models ***', end='')
    print('\n\n', end='')

    print('  - Building the logical data models for the SRC data layer... ',
          end='')
    sys.stdout.flush()
    logicalDataModels['SRC'] = SrcDataLayer(conf)
    print('Done!')

    print('  - Building the logical data models for the STG data layer... ',
          end='')
    sys.stdout.flush()
    logicalDataModels['STG'] = StgDataLayer(conf)
    print('Done!')

    print('  - Building the logical data models for the TRG data layer... ',
          end='')
    sys.stdout.flush()
    logicalDataModels['TRG'] = TrgDataLayer(conf)
    print('Done!')

    print('  - Building the logical data models for the SUM data layer... ',
          end='')
    sys.stdout.flush()
    logicalDataModels['SUM'] = SumDataLayer(conf)
    print('Done!')

    return logicalDataModels


def checkDBsForSuperflousTables(conf):
    query = ("SELECT table_name FROM information_schema.tables " +
             "WHERE table_schema = 'public'")

    etlDBCursor = conf.app.DWH_DATABASES['ETL'].cursor()
    etlDBCursor.execute(query)
    trgDBCursor = conf.app.DWH_DATABASES['TRG'].cursor()
    trgDBCursor.execute(query)

    # query returns list of tuples, (<tablename>, )
    allTables = []
    allTables.extend([item[0] for item in etlDBCursor.fetchall()])
    allTables.extend([item[0] for item in trgDBCursor.fetchall()])

    dataModelTables = []
    for dataLayerID in conf.state.LOGICAL_DATA_MODELS:
        dataModelTables.extend(
            conf.state.LOGICAL_DATA_MODELS[dataLayerID].getListOfTables())

    superflousTableNames = []
    for tableName in allTables:
        if tableName not in dataModelTables:
            superflousTableNames.append(tableName)

    if len(superflousTableNames) > 0:
        JOB_LOG.warn(
            logger.superflousTableWarning(',\n  '.join(superflousTableNames)))
