from . import utils
# from . import logger as logger
from . import dataflow

CONF = None


def init(appConfigFile, runTimeParams, scheduleConfig=None):
    global CONF
    CONF = utils.init(appConfigFile=appConfigFile,
                      runTimeParams=runTimeParams,
                      scheduleConfig=scheduleConfig)
    return CONF


def run():
    utils.run(CONF)


def DataFlow(desc):
    return dataflow.DataFlow(CONF, desc)


def getConf():
    global CONF
    return CONF
