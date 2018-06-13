from . import utils
from . import dataflow

# Users of BETL must call init() first, which sets up our Conf object.
# This script, main.py, holds that CONF object as a global variable.
# Executions are kicked off using the run() function - accessed via this
# script - and data manipulations are performed using DataFlow objects,
# which must also be initialisd via this script.
# Thus we can pass CONF to both without requiring the app developer to handle
# it

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
