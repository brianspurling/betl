import logging
import inspect
from datetime import datetime
import os
import psutil
import threading
from pathlib import Path
import sys
from time import sleep

JOB_LOG = None

EXEC_ID = None
LOG_LEVEL = logging.ERROR
CONF = None
JOB_LOG_FILE_NAME = None
PREVIOUS_MEM_USAGE = None

EXE_START_TIME = None

MEMORY_USAGE_LOOP = 'STOP'


def initialiseLogging(conf):

    execId = conf.STATE.EXEC_ID
    logLevel = conf.EXE.LOG_LEVEL

    global CONF
    global JOB_LOG
    global EXEC_ID
    global LOG_LEVEL
    global JOB_LOG_FILE_NAME
    global MEM_USAGE_AT_START

    CONF = conf
    EXEC_ID = execId
    if logLevel is not None:
        LOG_LEVEL = logLevel

    JOB_LOG_FILE_NAME = 'logs/' + str(EXEC_ID).zfill(4) + '_jobLog.log'

    JOB_LOG = logging.getLogger('JOB_LOG')
    jobLogFileName = JOB_LOG_FILE_NAME
    jobLogFileHandler = logging.FileHandler(jobLogFileName, mode='a')
    streamHandler = logging.StreamHandler()
    JOB_LOG.setLevel(logging.DEBUG)  # Always log everything on this log
    JOB_LOG.addHandler(jobLogFileHandler)
    JOB_LOG.addHandler(streamHandler)

    MEM_USAGE_AT_START = psutil.Process(os.getpid()).memory_info().rss

    return logging.getLogger('JOB_LOG')


def getLogger():
    return logging.getLogger('JOB_LOG')


def logBETLStart(conf):

    global EXE_START_TIME
    EXE_START_TIME = datetime.now()

    value = ''
    if conf.STATE.RERUN_PREV_JOB:
        value = 'Restarted'
    else:
        value = 'Started  '

    op = '\n'
    op += '                  *****************************' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *       BETL ' + value + '      *' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *****************************' + '\n'

    JOB_LOG.info(op)


def logBETLFinish(response):

    # Just in case (of error)
    global MEMORY_USAGE_LOOP
    MEMORY_USAGE_LOOP = 'STOP'

    op = '\n'
    op += '                  *****************************' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *       BETL Finished       *' + '\n'
    if response is not None:
        op += '                  *                           *' + '\n'
    if response == 'SUCCESS':
        op += '                  *   COMPLETED SUCCESSFULLY  *' + '\n'
    elif response == 'FAILED':
        op += '                  *     FAILED GRACEFULLY     *' + '\n'
    elif response == 'FAILED_RECOVERY':
        op += '                  * FAILED & DID NOT RECOVER  *' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *****************************' + '\n'

    JOB_LOG.info(op)


def logExecutionStart(conf):

    global EXE_START_TIME

    introText = 'Running NEW execution'
    if conf.STATE.RERUN_PREV_JOB:
        introText = 'Rerunning PREVIOUS execution'

    lastExecStatusMsg = ('The last execution (' +
                         str(conf.LAST_EXEC_REPORT['lastExecId']) + ') ' +
                         'finished with status: ' +
                         conf.LAST_EXEC_REPORT['lastExecStatus'])

    op = '\n'
    op += '----------------------------------------------------------------'
    op += '-------' + '\n'
    op += ' ' + introText + ': ' + str(conf.LAST_EXEC_REPORT['execId']) + '\n'
    op += '   - Started: ' + str(EXE_START_TIME) + '\n'
    op += '   - ' + lastExecStatusMsg + '\n'
    op += '-----------------------------------------------------------------'
    op += '-------' + '\n'

    JOB_LOG.info(op)


def logExecutionFinish():

    currentTime = datetime.now()
    elapsedSecs = (currentTime - EXE_START_TIME).total_seconds()
    elapsedMins = round(elapsedSecs / 60, 1)

    op = '\n'
    op += '                  Finished: ' + str(EXE_START_TIME)
    op += '\n'
    op += '                  Duration: ' + str(elapsedMins) + ' mins'
    op += '\n\n'
    op += '                       ' + JOB_LOG_FILE_NAME
    op += '\n'
    JOB_LOG.info(op)


def logAlerts():

    # We tag on any alerts to the end of the logs
    op = '\n'
    op += '*** ALERTS ***'
    op += '\n\n'
    alertsText = ''

    alertsFileName = 'logs/' + str(EXEC_ID).zfill(4) + '_alerts.txt'
    file = Path(alertsFileName)
    if file.is_file():
        with open(alertsFileName, 'r') as f:
            alertsText = f.read()

    if len(alertsText) == 0:
        alertsText = 'No alerts generated in this execution'

    op += alertsText
    op += '\n'

    JOB_LOG.info(op)


def logSetupFinish():

    op = ''
    op += '\n'
    op += '                 ------------------------------' + '\n'
    op += '                 |         Reset BETL         |' + '\n'
    op += '                 ------------------------------' + '\n'

    JOB_LOG.info(op)


def logRebuildPhysicalSchemaStart():

    op = ''
    op += '\n'
    op += '                 -------------------------------' + '\n'
    op += '                 | Rebuilding Physical Schemas |' + '\n'

    JOB_LOG.info(op)


def logRebuildPhysicalSchemaFinish():

    op = ''
    op += '\n'
    op += '                 |  Physical Schemas Rebuilt  |' + '\n'
    op += '                 ------------------------------' + '\n'

    JOB_LOG.info(op)


def logClearTempDataFinish():

    op = ''
    op += '\n'
    op += '                 ------------------------------' + '\n'
    op += '                 |    Deleted all temp data   |' + '\n'
    op += '                 ------------------------------' + '\n'

    JOB_LOG.info(op)


def logCheckLastModTimeOfSchemaDescGSheet():
    op = ''
    op += '  - Checking last modified date of the schema desc Gsheets'
    JOB_LOG.info(op)


def logInitialiseDatastore(datastoreID, datastoreType, isSchemaDesc=False):

    if datastoreID not in ['CTL']:
        desc = '  - Connecting to the ' + datastoreID + ' ' + \
             datastoreType + ' datastore'
        if isSchemaDesc:
            desc = '    - Connecting to the ' + datastoreID + ' schema ' + \
                'description spreadsheet'
        op = desc

        if JOB_LOG is not None:
            JOB_LOG.info(op)
        else:
            print(op)


def logInitialiseSrcSysDatastore(datastoreID, datastoreType):

    op = ''
    op += '\n'
    op += '  - Connecting to source system datastore: ' + datastoreID
    op += ' (' + datastoreType + ')'
    op += '\n'

    JOB_LOG.info(op)


def logDFStart(desc, startTime):

    stage = CONF.STATE.STAGE
    filename = os.path.basename(inspect.stack()[3][1]).replace('.py', '')
    funcname = inspect.stack()[3][3].replace("'", "")

    startStr = startTime.strftime('%H:%M:%S')

    callstack = stage + ' | ' + filename + '.' + funcname + ' | ' + startStr
    spacer = ' ' * (62 - len(callstack))
    spacer2 = ' ' * (59 - len(desc))

    op = ''
    op += '\n'
    op += '*****************************************************************\n'
    op += '*                                                               *\n'
    op += '* ' + callstack + spacer + '*\n'
    op += '*    ' + desc + spacer2 + '*\n'
    op += '*                                                               *\n'
    op += '*****************************************************************\n'

    JOB_LOG.info(op)


def logStepStart(startTime,
                 desc=None,
                 datasetName=None,
                 df=None,
                 additionalDesc=None):

    global MEMORY_USAGE_LOOP

    op = ''
    op += '   -------------------------------------------------------\n'
    op += '   | Operation: ' + str(inspect.stack()[2][3]) + '\n'
    if desc is not None:
        op += '   | Desc: "' + desc + '"\n'
    if additionalDesc is not None:
        op += '   | "' + additionalDesc + '"\n'
    if df is not None:
        op += describeDataFrame(df, datasetName, isPartOfStepLog=True)
    startStr = startTime.strftime('%H:%M:%S')
    op += '   | [Started step: ' + startStr + ']'

    JOB_LOG.info(op)

    if CONF.EXE.MONITOR_MEMORY_USAGE:
        MEMORY_USAGE_LOOP = 'GO'
        t = MemoryUsageThread()
        t.start()


def logStepError(str):

    op = ''
    op += '\n'
    op += '\n'
    op += str
    op += '\n'
    op += '\n'

    JOB_LOG.info(op)


def logStepEnd(report, duration, datasetName=None, df=None, shapeOnly=False):

    global MEMORY_USAGE_LOOP

    # Log step start would have finished by kicking off a separate thread
    # to monitor memory usage and o/p to console. We need to kill this now.
    if CONF.EXE.MONITOR_MEMORY_USAGE:
        MEMORY_USAGE_LOOP = 'STOP'

    # Need to give the thread time to kill itself tiddly (i.e. remove its
    # last log message)
    if CONF.EXE.MONITOR_MEMORY_USAGE:
        while True:
            sleep(0.01)
            if MEMORY_USAGE_LOOP == 'STOPPED':
                break

    op = ''
    op += '   | [Completed in: ' + str(round(duration, 2)) + ' seconds] \n'
    if report is not None and len(report) > 0:
        op += '   | Report: ' + report + '\n'

    if df is not None:
        op += describeDataFrame(
            df,
            datasetName,
            isPartOfStepLog=True,
            shapeOnly=shapeOnly)
    op += '   -------------------------------------------------------\n'

    JOB_LOG.info(op)


def logDFEnd(durationSeconds, df=None):

    op = ''
    op += '\n[Completed dataflow in: '
    op += str(round(durationSeconds, 2)) + ' seconds] \n\n'

    if df is not None:
        op += describeDataFrame(df)

    JOB_LOG.info(op)


def describeDataFrame(df,
                      datasetName=None,
                      isPartOfStepLog=False,
                      shapeOnly=False):

    firstChar = ''
    if isPartOfStepLog:
        firstChar = '| '

    tableContainsAuditCols = False
    numberOfColumns = len(df.columns.values)
    if set(CONF.DATA.AUDIT_COLS['colNames']).issubset(list(df.columns.values)):
        tableContainsAuditCols = True
        # We should be able to predict the number of audit cols, but that
        # doesn't help much with debugging (which, at this stage, is pretty
        # necessary with the audit functionality).
        numberOfAuditCols = 0
        for col in list(df.columns.values):
            if col in list(CONF.DATA.AUDIT_COLS['colNames']):
                numberOfAuditCols += 1
        numberOfColumns = numberOfColumns - numberOfAuditCols

    op = ''
    op += '   ' + firstChar + 'Output: ' + str(df.shape[0]) + ' rows, '
    op += str(numberOfColumns) + ' cols'
    if tableContainsAuditCols:
        op += ' (& ' + str(numberOfAuditCols) + ' audit cols)'
    if datasetName is not None:
        op += ' [' + datasetName + ']'
    op += '\n'
    if not shapeOnly:
        op += '   ' + firstChar + 'Columns:\n'
        for colName in list(df.columns.values):
            if colName not in CONF.DATA.AUDIT_COLS['colNames'].tolist():
                if len(str(colName)) > 30:
                    op += '   ' + firstChar + '   '
                    op += str(colName)[:30] + '--: '
                else:
                    op += '   ' + firstChar + '   ' + str(colName) + ': '

                value = getSampleValue(df, colName, 0)
                if value is not None:
                    op += value + ', '
                # value = getSampleValue(df, colName, 1)
                # if value is not None:
                #     op += value + ', '
                # value = getSampleValue(df, colName, 2)
                # if value is not None:
                #     op += value
                if len(df.index) > 1:
                    op += ', ...'
                op += '\n'
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


def logDeleteSrcSchemaDescWsFromSS():
    op = ''
    op += '  - Deleting all SRC worksheets from ETL Schema Desc Gsheet'
    JOB_LOG.info(op)


def logAutoPopSchemaDescsFromSrcStart():
    op = ''
    op += '\n'
    op += '                 -------------------------------' + '\n'
    op += '                 | Autopop SRC datalayer schema |' + '\n'
    op += '                 | descriptions from src system |' + '\n'
    JOB_LOG.info(op)


def logAutoPopSchemaDescsFromSrcFinish():
    op = ''
    op += '                 |    Schema desc populated   |' + '\n'
    op += '                 ------------------------------' + '\n'

    JOB_LOG.info(op)


def logRefreshingSchemaDescsFromGsheets(dbCount):
    op = ''
    op += '  - Refreshing the schema descriptions for ' + str(dbCount) + ' '
    op += 'databases from Google Sheets'
    JOB_LOG.info(op)


def logLoadingDBSchemaDescsFromGsheets(dbID):
    op = ''
    op += '    - Extracting schema descriptions for the ' + dbID + ' database...'
    JOB_LOG.info(op)


def logRefreshingSchemaDescsFromGsheets_done():
    op = ''
    op += '\n'
    JOB_LOG.info(op)


def logRebuildingPhysicalDataModel(dataLayerID):
    op = ''
    op += '    - Rebuilding the ' + dataLayerID + ' physical data models... '
    JOB_LOG.info(op)


def logVariancesReport():
    op = ''
    op += '\n'
    op += '*** REPORTS ***'
    JOB_LOG.info(op)


def logNoVariancesReported(varianceLimit):
    op = ''
    op += '\n'
    op += 'All step variances for this execution were within \n'
    op += str(varianceLimit) + ' standard deviations of the prior average'
    op += '\n'
    JOB_LOG.info(op)


def logSomeVariancesReported(varianceLimit, url):
    op = ''
    op += '\n'
    op += 'Some step variances for this execution were greater than \n'
    op += str(varianceLimit) + ' standard deviations of the prior average. \n'
    op += 'View the report here: ' + url
    op += '\n'
    JOB_LOG.info(op)


class MemoryUsageThread(threading.Thread):
    def __init__(self, name='MemoryUsageLogger'):
        threading.Thread.__init__(self, name=name)

    def run(self):
        global MEMORY_USAGE_LOOP
        while True:
            if MEMORY_USAGE_LOOP == 'STOP':
                # Overwrite the line without creating a lb
                print(' ' * 100, end='\r')
                MEMORY_USAGE_LOOP = 'STOPPED'
                sys.exit()
            else:
                sleep(0.1)  # pause for half a second
                currentMemUsage = psutil.Process(os.getpid()).memory_info().rss
                percent = round(currentMemUsage / MEM_USAGE_AT_START * 100)
                op = 'Used Memory: ' + str(percent) + \
                     '% (as % of use at start' + \
                     ') (' + datetime.now().strftime("%H:%M:%S") + ')'
                print(op, end='\r')
