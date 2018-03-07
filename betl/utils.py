import sys
import shutil
import os
import datetime
import time

from . import cli
from . import logger
from .dataLayer import SrcDataLayer
from .dataLayer import StgDataLayer
from .dataLayer import TrgDataLayer
from .dataLayer import SumDataLayer


def setUpExecution(conf, ctlDB):

    # Log in to the CTL DB and check the status of the last run
    lastExecDetails = getDetailsOfLastExecution(ctlDB)
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
                text = input(cli.CANT_RERUN_AND_SETUP_OR_REBUILD)
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
        ctlDB.insertNewExecutionToCtlTable(execId, conf.exe.BULK_OR_DELTA)

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


def setupBetl(ctlDB, conf):
    ctlDB.dropAllCtlTables()
    archiveLogFiles(conf)
    ctlDB.createExecutionsTable()
    ctlDB.createSchedulesTable()


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
    # TODO: Can't use logger.logStepStart here because we don't have the log
    # file set up (it needs the job ID). Maybe when I sort out jobLog
    # semantics, I can also move the log to the outisde of the whole execution
    # (execution ID, rather than job ID?). I think the job log should capture
    # everything, becuase an auto run could easily fail here.

    logicalDataModels = {}

    print('\n', end='')
    print('Loading the logical data models', end='')
    print('\n\n', end='')

    anythingLoaded = False

    if conf.exe.RUN_REBUILD_ALL or \
       conf.exe.RUN_REBUILD_SRC or \
       conf.exe.RUN_EXTRACT or     \
       conf.state.RERUN_PREV_JOB:
        print('  - Building the SRC logical data models... ', end='')
        sys.stdout.flush()
        logicalDataModels['SRC'] = SrcDataLayer(conf)
        print('Done!')
        anythingLoaded = True

    if conf.exe.RUN_REBUILD_ALL or \
       conf.exe.RUN_REBUILD_STG or \
       conf.exe.RUN_TRANSFORM or   \
       conf.state.RERUN_PREV_JOB:
        print('  - Building the STG logical data models... ', end='')
        sys.stdout.flush()
        logicalDataModels['STG'] = StgDataLayer(conf)
        print('Done!')
        anythingLoaded = True

    if conf.exe.RUN_REBUILD_ALL or \
       conf.exe.RUN_REBUILD_TRG or \
       conf.exe.RUN_LOAD or        \
       conf.state.RERUN_PREV_JOB:
        print('  - Building the TRG logical data models... ', end='')
        sys.stdout.flush()
        logicalDataModels['TRG'] = TrgDataLayer(conf)
        print('Done!')

        print('  - Building the SUM logical data models... ', end='')
        sys.stdout.flush()
        logicalDataModels['SUM'] = SumDataLayer(conf)
        print('Done!')
        anythingLoaded = True

    if not anythingLoaded:
        print('  - This execution did not require any logical ' +
              'models to be loaded')

    return logicalDataModels


def checkDBsForSuperflousTables(conf, logicalDataModels, jobLog):
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
    for dataLayerID in logicalDataModels:
        dataModelTables.extend(
            logicalDataModels[dataLayerID].getListOfTables())

    superflousTableNames = []
    for tableName in allTables:
        if tableName not in dataModelTables:
            superflousTableNames.append(tableName)

    if len(superflousTableNames) > 0:
        jobLog.warn(
            logger.superflousTableWarning(', '.join(superflousTableNames)))
