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
# def readData(tableName, dataLayerID, forceDBRead=None):
#     return DATA_IO.readData(tableName, dataLayerID, forceDBRead)
#
#
# def writeData(df, tableName, dataLayerID, append_or_replace='replace',
#               forceDBWrite=False):
#     DATA_IO.writeData(df, tableName, dataLayerID, append_or_replace,
#                       forceDBWrite)
#
#
# def getColumnHeadings(tableName, dataLayerID):
#     return DATA_IO.getColumnHeadings(tableName, dataLayerID)
#
#
# def customSql(sql, dataLayerID):
#     return DATA_IO.customSql(sql, dataLayerID)
#
#
# def readDataFromSrcSys(srcSysID, file_name_or_table_name):
#     return DATA_IO.readDataFromSrcSys(
#         srcSysID=srcSysID,
#         file_name_or_table_name=file_name_or_table_name)
#
#
# def getSKMapping(tableName, nkColList, skColName):
#     DATA_IO.getSKMapping(tableName, nkColList, skColName)
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
