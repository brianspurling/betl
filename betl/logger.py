from . import conf
import logging as logging
import inspect
import sys

LOG_FILE = None


def setUpLogger(moduleCode, name):
    logger = logging.getLogger(name)
    syslog = logging.StreamHandler()
    formatter = logging.Formatter('%(module_code)s.%(funcName)s: %(message)s')
    syslog.setFormatter(formatter)
    logger.setLevel(conf.LOG_LEVEL)
    logger.addHandler(syslog)
    return logging.LoggerAdapter(logger, {'module_code': moduleCode})


def createJobLogFile(jobId):
    global LOG_FILE

    LOG_FILE = open('logs/log_' + str(jobId) + '.txt', 'a')


def appendLogToFile(lgStr):
    global LOG_FILE
    LOG_FILE.write(lgStr)


def getLogFileName():
    return LOG_FILE.name


def logExecutionStartFinish(startOrFinish='START'):
    value = 'Started '
    if startOrFinish == 'FINISH':
        value = 'Finished'

    op = ''

    op += '\n'
    op += '\n'
    op += '\n'
    op += '                  *****************************' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *  BETL Execution ' + value + '  *' + '\n'
    op += '                  *                           *' + '\n'
    if startOrFinish == 'FINISH':
        op += '                  *      ' + getLogFileName()
        op += '     *'
        op += '\n'
        op += '                  *                           *' + '\n'
    op += '                  *****************************' + '\n'
    # op += 'Arguments: ' + '\n'
    # op += '\n'
    # op += pprint.pformat(args)
    # op += '\n'
    print(op)
    sys.stdout.flush()
    # appendLogToFile(op)


def logStartOfJobExecution(jobId):
    op = ''
    op += '\n\n'
    op += '-------------------' + '\n'
    op += ' Running job ' + str(jobId) + '\n'
    op += '-------------------' + '\n'
    print(op)
    sys.stdout.flush()


def logStepStart(stepDescription, stepId=None, callingFuncName=None):
    op = ''
    op += '\n'
    op += '******************************************************************'
    op += '\n'
    op += '\n'

    stage = '[' + conf.STAGE + '] '
    funcName = callingFuncName if callingFuncName is not None else            \
        inspect.stack()[1][3]

    if (stepId is not None):
        op += stage + funcName + ' (step ' + str(stepId) + '): '
    else:
        op += stage + funcName + ': '
    op += stepDescription + '\n'
    print(op)
    sys.stdout.flush()
    appendLogToFile(op)


def logStepEnd(df):
    op = ''
    op += describeDataFrame(df)

    print(op)
    sys.stdout.flush()
    appendLogToFile(op)


def describeDataFrame(df):
    op = ''
    op += 'Shape: ' + str(df.shape) + '\n'
    op += '\n'
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
