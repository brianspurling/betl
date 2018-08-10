import logging
import inspect
from datetime import datetime
import os
import psutil
import threading
from pathlib import Path
import sys
from time import sleep


class Logger():

    # Class Attributes #

    # These are set in initialiseLogging, which is run during the
    # initialisation of a pipeline
    EXEC_ID = None
    LOG_LEVEL = None
    JOB_LOG_FILE_NAME = None
    MEMORY_USAGE_LOOP = None
    MEM_USAGE_AT_START = None
    AUDIT_COLS = None
    EXE_START_TIME = None

    JOB_LOG = None

    # TODO: mem loop class is probably wrong, need to check other globals and
    #       change to class vars, need an init func, need to change way
    #       pipeline (and all?) call this

    def __init__(self):
        pass

    # This function sets only the class attributes
    def initialiseLogging(execId, logLevel, logPath, auditCols):

        Logger.EXEC_ID = execId

        if logLevel is not None:
            Logger.LOG_LEVEL = logLevel
        else:
            Logger.LOG_LEVEL = logging.ERROR

        Logger.JOB_LOG_FILE_NAME = \
            logPath + '/' + str(Logger.EXEC_ID).zfill(4) + '_jobLog.log'

        Logger.MEMORY_USAGE_LOOP = 'STOP'

        Logger.MEM_USAGE_AT_START = \
            psutil.Process(os.getpid()).memory_info().rss

        Logger.AUDIT_COLS = auditCols

        # Finally, set up our logging object
        Logger.JOB_LOG = logging.getLogger('JOB_LOG')
        jobLogFileHandler = \
            logging.FileHandler(Logger.JOB_LOG_FILE_NAME, mode='a')
        streamHandler = logging.StreamHandler()
        Logger.JOB_LOG.setLevel(logging.DEBUG)
        Logger.JOB_LOG.addHandler(jobLogFileHandler)
        Logger.JOB_LOG.addHandler(streamHandler)

    def logBETLStart(self, rerunPrevJob):

        Logger.EXE_START_TIME = datetime.now()

        value = ''
        if rerunPrevJob:
            value = 'Restarted'
        else:
            value = 'Started  '

        op = '\n'
        op += '                  *****************************' + '\n'
        op += '                  *                           *' + '\n'
        op += '                  *       BETL ' + value + '      *' + '\n'
        op += '                  *                           *' + '\n'
        op += '                  *****************************' + '\n'

        Logger.JOB_LOG.info(op)

    def logBETLFinish(self, response):

        # Just in case (of error)
        Logger.MEMORY_USAGE_LOOP = 'STOP'
        op = '\n'
        op += '                  *****************************' + '\n'
        op += '                  *                           *' + '\n'
        op += '                  *       BETL Finished       *' + '\n'
        if response is not None:
            op += '                  *                           *' + '\n'
        if response == 'SUCCESS':
            op += '                  *   COMPLETED SUCCESSFULLY  *' + '\n'
        elif response == 'FAIL':
            op += '                  *     FAILED GRACEFULLY     *' + '\n'
        elif response == 'FAILED_RECOVERY':
            op += '                  * FAILED & DID NOT RECOVER  *' + '\n'
        op += '                  *                           *' + '\n'
        op += '                  *****************************' + '\n'

        Logger.JOB_LOG.info(op)

    def logExecutionStart(self, rerunPrevJob, lastExecReport):

        introText = 'Running NEW execution'
        if rerunPrevJob:
            introText = 'Rerunning PREVIOUS execution'

        lastExecStatusMsg = ('The last execution (' +
                             str(lastExecReport['lastExecId']) + ') ' +
                             'finished with status: ' +
                             lastExecReport['lastExecStatus'])

        op = '\n'
        op += '----------------------------------------------------------------'
        op += '-------' + '\n'
        op += ' ' + introText + ': ' + str(lastExecReport['execId']) + '\n'
        op += '   - Started: ' + str(Logger.EXE_START_TIME) + '\n'
        op += '   - ' + lastExecStatusMsg + '\n'
        op += '-----------------------------------------------------------------'
        op += '-------' + '\n'

        Logger.JOB_LOG.info(op)

    def logExecutionFinish(self):

        currentTime = datetime.now()
        elapsedSecs = (currentTime - Logger.EXE_START_TIME).total_seconds()
        elapsedMins = round(elapsedSecs / 60, 1)

        op = '\n'
        op += '                  Finished: ' + str(Logger.EXE_START_TIME)
        op += '\n'
        op += '                  Duration: ' + str(elapsedMins) + ' mins'
        op += '\n\n'
        op += '                       ' + Logger.JOB_LOG_FILE_NAME
        op += '\n'
        Logger.JOB_LOG.info(op)

    def logAlerts(self):

        # We tag on any alerts to the end of the logs
        op = '\n'
        op += '*** ALERTS ***'
        op += '\n\n'
        alertsText = ''

        alertsFileName = 'logs/' + str(Logger.EXEC_ID).zfill(4) + '_alerts.txt'
        file = Path(alertsFileName)
        if file.is_file():
            with open(alertsFileName, 'r') as f:
                alertsText = f.read()

        if len(alertsText) == 0:
            alertsText = 'No alerts generated in this execution'

        op += alertsText
        op += '\n'

        Logger.JOB_LOG.info(op)

    def logSetupFinish(self):

        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |         Reset BETL         |' + '\n'
        op += '                 ------------------------------' + '\n'

        Logger.JOB_LOG.info(op)

    def logRebuildPhysicalSchemaStart(self):

        op = ''
        op += '\n'
        op += '                 -------------------------------' + '\n'
        op += '                 | Rebuilding Physical Schemas |' + '\n'

        Logger.JOB_LOG.info(op)

    def logRebuildPhysicalSchemaFinish(self):

        op = ''
        op += '\n'
        op += '                 |  Physical Schemas Rebuilt  |' + '\n'
        op += '                 ------------------------------' + '\n'

        Logger.JOB_LOG.info(op)

    def logClearTempDataFinish(self):

        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |    Deleted all temp data   |' + '\n'
        op += '                 ------------------------------' + '\n'

        Logger.JOB_LOG.info(op)

    def logCheckLastModTimeOfSchemaDescGSheet(self):
        op = ''
        op += '  - Checking last modified date of the schema desc Gsheets'
        Logger.JOB_LOG.info(op)

    def logInitialiseDatastore(self, datastoreID, datastoreType, isSchemaDesc=False):

        if datastoreID not in ['CTL']:
            desc = '  - Connecting to the ' + datastoreID + ' ' + \
                 datastoreType + ' datastore'
            if isSchemaDesc:
                desc = '    - Connecting to the ' + datastoreID + ' schema ' + \
                    'description spreadsheet'
            op = desc

            if Logger.JOB_LOG is not None:
                Logger.JOB_LOG.info(op)
            else:
                print(op)

    def logInitialiseSrcSysDatastore(self, datastoreID, datastoreType):

        op = ''
        op += '\n'
        op += '  - Connecting to source system datastore: ' + datastoreID
        op += ' (' + datastoreType + ')'
        op += '\n'

        Logger.JOB_LOG.info(op)

    def logDFStart(self, desc, startTime, stage):

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

        Logger.JOB_LOG.info(op)

    def logStepStart(self,
                     startTime,
                     desc=None,
                     datasetName=None,
                     df=None,
                     additionalDesc=None,
                     monitorMemoryUsage=False):

        op = ''
        op += '   -------------------------------------------------------\n'
        op += '   | Operation: ' + str(inspect.stack()[2][3]) + '\n'
        if desc is not None:
            op += '   | Desc: "' + desc + '"\n'
        if additionalDesc is not None:
            op += '   | "' + additionalDesc + '"\n'
        if df is not None:
            op += self.describeDataFrame(df, datasetName, isPartOfStepLog=True)
        startStr = startTime.strftime('%H:%M:%S')
        op += '   | [Started step: ' + startStr + ']'

        Logger.JOB_LOG.info(op)

        if monitorMemoryUsage:
            Logger.MEMORY_USAGE_LOOP = 'GO'
            t = MemoryUsageThread()
            t.start()

    def logStepError(self, str):

        op = ''
        op += '\n'
        op += '\n'
        op += str
        op += '\n'
        op += '\n'

        Logger.JOB_LOG.info(op)

    def logStepEnd(self,
                   report,
                   duration,
                   datasetName=None,
                   df=None,
                   shapeOnly=False,
                   monitorMemoryUsage=False):

        # Log step start would have finished by kicking off a separate thread
        # to monitor memory usage and o/p to console. We need to kill this now.
        if monitorMemoryUsage:
            Logger.MEMORY_USAGE_LOOP = 'STOP'

        # Need to give the thread time to kill itself tiddly (i.e. remove its
        # last log message)
        if monitorMemoryUsage:
            while True:
                sleep(0.01)
                if Logger.MEMORY_USAGE_LOOP == 'STOPPED':
                    break

        op = ''
        op += '   | [Completed in: ' + str(round(duration, 2)) + ' seconds] \n'
        if report is not None and len(report) > 0:
            op += '   | Report: ' + report + '\n'

        if df is not None:
            op += self.describeDataFrame(
                df,
                datasetName,
                isPartOfStepLog=True,
                shapeOnly=shapeOnly)
        op += '   -------------------------------------------------------\n'

        Logger.JOB_LOG.info(op)

    def logDFEnd(self, durationSeconds, df=None):

        op = ''
        op += '\n[Completed dataflow in: '
        op += str(round(durationSeconds, 2)) + ' seconds] \n\n'

        if df is not None:
            op += self.describeDataFrame(df)

        Logger.JOB_LOG.info(op)

    def describeDataFrame(self,
                          df,
                          datasetName=None,
                          isPartOfStepLog=False,
                          shapeOnly=False):

        firstChar = ''
        if isPartOfStepLog:
            firstChar = '| '

        tableContainsAuditCols = False
        numberOfColumns = len(df.columns.values)
        if set(Logger.AUDIT_COLS['colNames']).issubset(list(df.columns.values)):
            tableContainsAuditCols = True
            # We should be able to predict the number of audit cols, but that
            # doesn't help much with debugging (which, at this stage, is pretty
            # necessary with the audit functionality).
            numberOfAuditCols = 0
            for col in list(df.columns.values):
                if col in list(Logger.AUDIT_COLS['colNames']):
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
                if colName not in Logger.AUDIT_COLS['colNames'].tolist():
                    if len(str(colName)) > 30:
                        op += '   ' + firstChar + '   '
                        op += str(colName)[:30] + '--: '
                    else:
                        op += '   ' + firstChar + '   ' + str(colName) + ': '

                    value = self.getSampleValue(df, colName, 0)
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

    def getSampleValue(self, df, colName, rowNum):
        if len(df.index) >= rowNum + 1:
            value = str(df[colName].iloc[rowNum])
            value = value.replace('\n', '')
            value = value.replace('\t', '')
            if len(value) > 20:
                value = value[0:30] + '..'
        else:
            value = None
        return value

    def logDeleteSrcSchemaDescWsFromSS(self):
        op = ''
        op += '  - Deleting all SRC worksheets from ETL Schema Desc Gsheet'
        Logger.JOB_LOG.info(op)

    def logAutoPopSchemaDescsFromSrcStart(self):
        op = ''
        op += '\n'
        op += '                 -------------------------------' + '\n'
        op += '                 | Autopop SRC datalayer schema |' + '\n'
        op += '                 | descriptions from src system |' + '\n'
        Logger.JOB_LOG.info(op)

    def logAutoPopSchemaDescsFromSrcFinish(self):
        op = ''
        op += '                 |    Schema desc populated   |' + '\n'
        op += '                 ------------------------------' + '\n'

        Logger.JOB_LOG.info(op)

    def logRefreshingSchemaDescsFromGsheets(self, dbCount):
        op = ''
        op += '  - Refreshing the schema descriptions for ' + str(dbCount) + ' '
        op += 'databases from Google Sheets'
        Logger.JOB_LOG.info(op)

    def logLoadingDBSchemaDescsFromGsheets(self, dbID):
        op = ''
        op += '    - Extracting schema descriptions for the ' + dbID
        op += ' database...'
        Logger.JOB_LOG.info(op)

    def logRefreshingSchemaDescsFromGsheets_done(self):
        op = ''
        op += '\n'
        Logger.JOB_LOG.info(op)

    def logRebuildingPhysicalDataModel(self, dataLayerID):
        op = ''
        op += '    - Rebuilding the ' + dataLayerID + ' physical data models... '
        Logger.JOB_LOG.info(op)

    def logVariancesReport(self):
        op = ''
        op += '\n'
        op += '*** REPORTS ***'
        Logger.JOB_LOG.info(op)

    def logNoVariancesReported(self, varianceLimit):
        op = ''
        op += '\n'
        op += 'All step variances for this execution were within \n'
        op += str(varianceLimit) + ' standard deviations of the prior average'
        op += '\n'
        Logger.JOB_LOG.info(op)

    def logSomeVariancesReported(self, varianceLimit, url):
        op = ''
        op += '\n'
        op += 'Some step variances for this execution were greater than \n'
        op += str(varianceLimit) + ' standard deviations of the prior average. \n'
        op += 'View the report here: ' + url
        op += '\n'
        Logger.JOB_LOG.info(op)


class MemoryUsageThread(threading.Thread):
    def __init__(self, name='MemoryUsageLogger'):
        threading.Thread.__init__(self, name=name)

    def run(self):
        while True:
            if Logger.MEMORY_USAGE_LOOP == 'STOP':
                # Overwrite the line without creating a lb
                print(' ' * 100, end='\r')
                Logger.MEMORY_USAGE_LOOP = 'STOPPED'
                sys.exit()
            else:
                sleep(0.1)  # pause for half a second
                currentMemUsage = psutil.Process(os.getpid()).memory_info().rss
                percent = round(currentMemUsage / Logger.MEM_USAGE_AT_START * 100)
                op = 'Used Memory: ' + str(percent) + \
                     '% (as % of use at start' + \
                     ') (' + datetime.now().strftime("%H:%M:%S") + ')'
                print(op, end='\r')
