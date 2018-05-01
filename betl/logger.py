import logging
import inspect
from datetime import datetime
import pprint
import traceback


JOB_LOG = None

EXEC_ID = None
LOG_LEVEL = logging.ERROR
CONF = None
JOB_LOG_FILE_NAME = None

EXE_START_TIME = None


def initialiseLogging(conf):

    execId = conf.state.EXEC_ID
    logLevel = conf.exe.LOG_LEVEL

    global JOB_LOG
    global EXEC_ID
    global LOG_LEVEL
    global CONF
    global JOB_LOG_FILE_NAME

    EXEC_ID = execId
    if logLevel is not None:
        LOG_LEVEL = logLevel
    CONF = conf

    JOB_LOG_FILE_NAME = 'logs/' + str(EXEC_ID) + '_jobLog.log'

    JOB_LOG = logging.getLogger('JOB_LOG')
    jobLogFileName = JOB_LOG_FILE_NAME
    jobLogFileHandler = logging.FileHandler(jobLogFileName, mode='a')
    streamHandler = logging.StreamHandler()
    JOB_LOG.setLevel(logging.DEBUG)  # Always log everything on this log
    JOB_LOG.addHandler(jobLogFileHandler)
    JOB_LOG.addHandler(streamHandler)


def getLogger():
    return logging.getLogger('JOB_LOG')


def logExecutionStart(rerun=False):

    global EXE_START_TIME
    EXE_START_TIME = datetime.now()

    value = ''
    if rerun:
        value = 'Restarted'
    else:
        value = 'Started  '

    op = '\n'
    op += '                  *****************************' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *  BETL Execution ' + value + ' *' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *****************************' + '\n'

    return(op)


def logExecutionOverview(execReport, rerun=False):

    global EXE_START_TIME

    introText = 'Running NEW execution'
    if rerun:
        introText = 'Rerunning PREVIOUS execution'

    lastExecStatusMsg = ('The last execution (' +
                         str(execReport['lastExecId']) + ') ' +
                         'finished with status: ' +
                         execReport['lastExecStatus'])

    op = ''
    op += '----------------------------------------------------------------'
    op += '-----' + '\n'
    op += ' ' + introText + ': ' + str(execReport['execId']) + '\n'
    op += '   - Started: ' + str(EXE_START_TIME) + '\n'
    op += '   - ' + lastExecStatusMsg + '\n'
    op += '-----------------------------------------------------------------'
    op += '-----' + '\n'
    return op


def logDFStart(desc, startTime):

    op = ''
    op += '\n'
    op += '******************************************************************'
    op += '\n'
    stage = CONF.state.STAGE + ' | ' + '\n\n'
    op += '******************************************************************'
    op += '\n'

    op += pprint.pformat(traceback.print_stack())

    op += stage + desc + '\n\n'

    op += '[Started dataflow at: ' + str(startTime) + ']' + '\n'

    JOB_LOG.info(op)


def logDFEnd(durationSeconds, df=None):

    op = ''
    op += '[Completed dataflow in: '
    op += str(round(durationSeconds, 2)) + ' seconds] \n\n'

    if df is not None:
        op += describeDataFrame(df)

    JOB_LOG.info(op)


def logStepStart(startTime, desc=None):

    op = ''
    op += '   ' + str(inspect.stack()[1][3])
    if desc is not None:
        op += ': ' + desc + '\n'
    else:
        op += '\n'
    op += '   [Started at: ' + str(startTime) + ']'

    JOB_LOG.info(op)


def logStepEnd(report, duration, df=None):
    op = ''
    op += '   [Completed step in: ' + str(round(duration, 2)) + ' seconds] \n'
    op += '   ' + report + '\n'
    if df is not None:
        op += describeDataFrame(df)

    JOB_LOG.info(op)


def describeDataFrame(df):
    op = ''
    op += '   Shape of final dataset: ' + str(df.shape) + '\n\n'
    op += 'Columns:\n'
    for colName in list(df.columns.values):
        if len(str(colName)) > 30:
            op += ' ' + str(colName)[:30] + '--: '
        else:
            op += ' ' + str(colName) + ': '

        value = getSampleValue(df, colName, 0)
        if value is not None:
            op += value + ', '
        value = getSampleValue(df, colName, 1)
        if value is not None:
            op += value + ', '
        value = getSampleValue(df, colName, 2)
        if value is not None:
            op += value
        if len(df.index) > 3:
            op += ', ...'
        op += '\n'
    return op


def logExecutionFinish(rerun=False):

    op = '\n'
    op += '                  *****************************' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *  BETL Execution Finished  *' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *****************************' + '\n'

    currentTime = datetime.now()
    elapsedSecs = (currentTime - EXE_START_TIME).total_seconds()
    elapsedMins = round(elapsedSecs / 60, 1)

    op += '\n'
    op += '                  Finished: ' + str(EXE_START_TIME)
    op += '\n'
    op += '                  Duration: ' + str(elapsedMins) + ' mins'
    op += '\n\n'
    op += '                       ' + JOB_LOG_FILE_NAME
    op += '\n'

    return(op)


def logBetlSetupComplete():
    op = ''
    op += '\n'
    op += '-------------------------' + '\n'
    op += ' BETL setup successfully ' + '\n'
    op += '-------------------------' + '\n'
    return op


def logClearedTempData():
    op = ''
    op += '\n\n'
    op += '-----------------------' + '\n'
    op += ' Cleared all temp data ' + '\n'
    op += '-----------------------' + '\n'
    return op


def getSampleValue(df, colName, rowNum):
    if len(df.index) >= rowNum + 1:
        value = str(df[colName].iloc[rowNum])
        value = value.replace('\n', '')
        value = value.replace('\t', '')
        if len(value) > 20:
            value = value[0:30] + '..'
    else:
        value = None
    return value


def logUnableToReadFromCtlDB(errorMessage):
    op = '\n'
    op += '*** ERROR! Execution stopped *** \n'
    op += '\n'
    op += '  BETL tried to read from the CTL DB but failed. \n'
    op += '  This is probably because you haven\'t set up BETL yet - \n'
    op += '  run your application again with the setup arugment. \n'
    op += '  to create a fresh control (CTL) DB \n'
    op += '  \n'
    op += '  Or, if you think BETL is set up correctly, check the \n'
    op += '  CTL DB credentials in your appConfig file \n'
    op += '  \n'
    op += '  The error message was: \n'
    op += '  \n'
    op += errorMessage
    op += '  \n'
    return op


def superflousTableWarning(tableNamesStr):
    op = '\n'
    op += '*** WARNING! Superfluous tables in DB *** \n'
    op += '\n'
    op += '  The following tables were found in one of the databases \n'
    op += '  but not in the logical data model. They should be checked \n'
    op += '  and removed'
    op += '\n'
    op += '\n'
    op += '  ' + tableNamesStr
    op += '  \n'
    return op


def logPhysicalDataModelBuildStart():
    op = ''
    op += '*** Rebuilding the physical data models ***'
    op += '\n'
    return op


def logPhysicalDataModelBuild_dataLayerDone(dataLayerID):
    op = ''
    op += '  - Rebuilt the ' + dataLayerID + ' physical data models... '
    return op
