import os
import logging
import inspect
from datetime import datetime

# TODO: http://docs.python-guide.org/en/latest/writing/logging/
# >>> logging in a library

EXEC_ID = None
LOG_LEVEL = logging.ERROR
CONF = None
JOB_LOG_FILE_NAME = None
DEV_LOG_FILE_NAME = None

STEP_START_TIME = None
EXE_START_TIME = None

# TODO loggers allow some kind of ancestor/inheritance model


def initialiseLogging(execId, logLevel, conf):
    global EXEC_ID
    global LOG_LEVEL
    global CONF
    global JOB_LOG_FILE_NAME
    global DEV_LOG_FILE_NAME

    EXEC_ID = execId
    if logLevel is not None:
        LOG_LEVEL = logLevel
    CONF = conf

    JOB_LOG_FILE_NAME = 'logs/' + str(EXEC_ID) + '_jobLog.log'
    DEV_LOG_FILE_NAME = 'logs/' + str(EXEC_ID) + '_devLog.log'

    # if the files already exist, remove (a previous run might have failed
    # before it saved the new execId down to the DB, therefore we might be
    # running with the same execId again)
    # TODO: archive the old files!
    if (os.path.exists(JOB_LOG_FILE_NAME)):
        os.remove(JOB_LOG_FILE_NAME)
    if (os.path.exists(DEV_LOG_FILE_NAME)):
        os.remove(DEV_LOG_FILE_NAME)

    jobLog = logging.getLogger('JOB_LOG')
    jobLogFileName = JOB_LOG_FILE_NAME
    devLogFileName = DEV_LOG_FILE_NAME
    jobLogFileHandler = logging.FileHandler(jobLogFileName, mode='a')
    devLogFileHandler = logging.FileHandler(devLogFileName, mode='a')
    streamHandler = logging.StreamHandler()
    jobLog.setLevel(logging.DEBUG)  # Always log everything on this log
    jobLog.addHandler(jobLogFileHandler)
    jobLog.addHandler(devLogFileHandler)
    jobLog.addHandler(streamHandler)


def getJobLog():
    return logging.getLogger('JOB_LOG')


def getDevLog(moduleName):
    logger = logging.getLogger(moduleName)
    formatter = logging.Formatter('%(module)s.%(funcName)s: %(message)s')
    logger.setLevel(logging.DEBUG)  # we set the handler-specific levels later
    logFileName = DEV_LOG_FILE_NAME
    devLogFileHandler = logging.FileHandler(logFileName, mode='a')
    devLogFileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    streamHandler.setLevel(LOG_LEVEL)
    devLogFileHandler.setLevel(logging.DEBUG)

    logger.addHandler(devLogFileHandler)
    logger.addHandler(streamHandler)
    return logging.getLogger(moduleName)


def logExecutionStartFinish(startOrFinish='START', rerun=False):

    global EXE_START_TIME

    if startOrFinish == 'START':
        EXE_START_TIME = datetime.now()

    print('')
    value = ''
    if startOrFinish == 'START':
        if rerun:
            value = 'Restarted'
        else:
            value = 'Started  '
    if startOrFinish == 'FINISH':
        value = 'Finished '

    op = ''
    op += '                  *****************************' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *  BETL Execution ' + value + ' *' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *****************************' + '\n'
    if startOrFinish == 'FINISH':
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
        op += '                       ' + DEV_LOG_FILE_NAME
        op += '\n'
    # op += 'Arguments: ' + '\n'
    # op += '\n'
    # op += pprint.pformat(args)
    # op += '\n'
    return(op)


def logBetlSetupComplete():
    op = ''
    op += '\n'
    op += '-------------------------' + '\n'
    op += ' BETL setup successfully ' + '\n'
    op += '-------------------------' + '\n'
    return op


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


def logClearedTempData():
    op = ''
    op += '\n\n'
    op += '-----------------------' + '\n'
    op += ' Cleared all temp data ' + '\n'
    op += '-----------------------' + '\n'
    return op


def logStepStart(stepDescription, stepId=None, callingFuncName=None):
    global STEP_START_TIME
    STEP_START_TIME = datetime.now()

    op = ''
    op += '\n'
    op += '******************************************************************'
    op += '\n'
    op += '\n'

    stage = '[' + CONF.state.STAGE + '] '
    funcName = callingFuncName if callingFuncName is not None else            \
        inspect.stack()[2][3]

    if (stepId is not None):
        op += stage + funcName + ' (step ' + str(stepId) + ') '
    else:
        op += stage + funcName + ' '

    op += stepDescription + '\n\n'

    op += '[Started at: ' + str(STEP_START_TIME) + ']'

    return op


def logStepEnd(df=None):

    currentTime = datetime.now()
    elapsedSeconds = (currentTime - STEP_START_TIME).total_seconds()
    op = ''

    op += '[Completed in: ' + str(round(elapsedSeconds, 2)) + ' seconds]\n\n'

    if df is not None:
        op += describeDataFrame(df)
    else:
        op += ''

    return op


def describeDataFrame(df):
    op = ''
    op += 'Shape: ' + str(df.shape) + '\n\n'
    op += 'Columns:\n'
    for colName in list(df.columns.values):
        op += ' ' + colName + ': '
        op += getSampleValue(df, colName, 0) + ', '
        op += getSampleValue(df, colName, 1) + ', '
        op += getSampleValue(df, colName, 2)
        op += ', ... \n'
    return op


def getSampleValue(df, colName, rowNum):
    if len(df.index) >= rowNum + 1:
        value = str(df[colName].iloc[rowNum])
        value = value.replace('\n', '')
        value = value.replace('\t', '')
    else:
        value = ''
    if len(value) > 20:
        value = value[0:30] + '..'
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
