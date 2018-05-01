from . import utils
# from . import logger as logger
from . import dataflow

CONF = None


def init(appConfigFile, runTimeParams, scheduleConfig=None):
    global CONF
    CONF = utils.init(appConfigFile=appConfigFile,
                      runTimeParams=runTimeParams,
                      scheduleConfig=scheduleConfig)


def run():
    utils.run(CONF)


def DataFlow(desc):
    return dataflow.DataFlow(CONF, desc)

#
#
# def mergeFactWithSks(df, col):
#     return DATA_IO.mergeFactWithSks(df, col)
#
#
# def logStepStart(stepDescription, stepId=None, callingFuncName=None):
#     JOB_LOG.info(logger.logStepStart(stepDescription=stepDescription,
#                                      stepId=stepId,
#                                      callingFuncName=callingFuncName))
#
#
# def logStepEnd(df=None):
#     JOB_LOG.info(logger.logStepEnd(df=df))
