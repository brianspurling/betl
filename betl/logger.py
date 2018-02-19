from . import conf
import logging as logging
import pprint
import inspect

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


def logStepStart(stepDescription, stepId=None, callingFuncName=None):
    op = ''
    op += '\n'

    funcName = callingFuncName if callingFuncName is not None else            \
        inspect.stack()[1][3]

    if (stepId is not None):
        op += funcName + ' (step ' + str(stepId) + '): '
    else:
        op += funcName + ': '
    op += stepDescription + '\n'
    print(op)
    appendLogToFile(op)


def logStepEnd(df):
    op = ''
    op += 'Shape: ' + str(df.shape) + '\n'
    op += '\n'
    op += 'Columns: '
    op += pprint.pformat(list(df.columns.values))
    if len(df.columns.values) < 4:
        op += '\n\n'
        op += 'df.head >>> '
        op += '\n\n'
        op += pprint.pformat(df.head())
    op += '\n'
    op += '\n'
    op += '******************************************************************'

    print(op)
    appendLogToFile(op)
