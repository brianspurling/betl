import logging
import inspect
from datetime import datetime
import os
from pathlib import Path


class Logger():

    def __init__(self, conf):

        self.CONF = conf

        if conf.LOG_LEVEL is not None:
            self.LOG_LEVEL = conf.LOG_LEVEL
        else:
            self.LOG_LEVEL = logging.ERROR

        if conf.IS_ADMIN:
            fileNamePrefix = 'admin_'
        if conf.IS_AIRFLOW:
            fileNamePrefix = 'airflow_' + conf.EXEC_ID + '_'
        else:
            # TODO: get last manual file, ID, and increment and add to filename
            fileNamePrefix = 'manual_'

        self.JOB_LOG_FILE_NAME = (conf.LOG_PATH + '/' +
                                  fileNamePrefix +
                                  'jobLog.log')

        # Finally, set up our logging object
        self.JOB_LOG = logging.getLogger('JOB_LOG')
        jobLogFileHandler = \
            logging.FileHandler(self.JOB_LOG_FILE_NAME, mode='a')
        # streamHandler = logging.StreamHandler()
        self.JOB_LOG.setLevel(logging.DEBUG)
        self.JOB_LOG.addHandler(jobLogFileHandler)
        # self.JOB_LOG.addHandler(streamHandler)

    def logBETLStart(self, test):

        op = '\n'
        op += '                  *****************************' + '\n'
        op += '                  *                           *' + '\n'
        op += '                  *        BETL Started       *' + '\n'
        op += '                  *                           *' + '\n'
        op += '                  *****************************' + '\n'
        self.JOB_LOG.info(op)

    def logResetStart(self):

        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |       Resetting BETL       |' + '\n'
        op += '                 |                            |' + '\n'
        op += '                 |  This will...              |' + '\n'
        op += '                 |    - archive all log files |' + '\n'
        op += '                 |    - wipe the reports dir  |' + '\n'
        op += '                 |    - wipe the schema dir.. |' + '\n'
        op += '                 |    - ..but preserve the    |' + '\n'
        op += '                 |      srcTableName mapping  |' + '\n'

        self.JOB_LOG.info(op)

    def logResetEnd(self):

        op = ''
        op += '\n'
        op += '                 |         BETL Reset         |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBETLEnd(self):

        # Just in case (of error)
        op = '\n'
        op += '                  *****************************' + '\n'
        op += '                  *                           *' + '\n'
        op += '                  *       BETL Finished       *' + '\n'
        # if response is not None:
        #     op += '                  *                           *' + '\n'
        # if response == 'SUCCESS':
        #     op += '                  *   COMPLETED SUCCESSFULLY  *' + '\n'
        # elif response == 'FAIL':
        #     op += '                  *     FAILED GRACEFULLY     *' + '\n'
        # elif response == 'FAILED_RECOVERY':
        #     op += '                  * FAILED & DID NOT RECOVER  *' + '\n'
        op += '                  *                           *' + '\n'
        op += '                  *****************************' + '\n'

        self.JOB_LOG.info(op)

    def logDeleteTemporaryDataEnd(self):

        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |    Deleted all temp data   |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logPopulateTempDataFileNameMapEnd(self):

        op = ''
        op += '\n'
        op += '                 -------------------------------' + '\n'
        op += '                 |  Created tmp data file map  |' + '\n'
        op += '                 -------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logAutoPopExtSchemaDescGSheetsStart(self):
        op = ''
        op += '\n'
        op += '                 --------------------------------' + '\n'
        op += '                 | Autopop SRC datalayer schema |' + '\n'
        op += '                 | descriptions from src system |' + '\n'
        op += '                 |                              |' + '\n'
        op += '                 |  This will...                |' + '\n'
        op += '                 |    - Read all src sys schemas|' + '\n'
        op += '                 |    - Del all EXT worksheets  |' + '\n'
        op += '                 |      from DB Schema GSheet    |' + '\n'
        op += '                 |    - Recreate with src schema|' + '\n'
        self.JOB_LOG.info(op)

    def logAutoPopExtSchemaDescsEnd(self):
        op = ''
        op = '\n'
        op += '                 |    Schema desc populated   |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logReadingSrcSysSchema(self, ssID):

        op = ''
        op += '  - Reading schema of source system ' + ssID
        op += '\n'
        self.JOB_LOG.info(op)

    def logDeleteSrcSchemaDescWsFromSS(self):
        op = ''
        op += '  - Deleting all SRC worksheets from ETL Schema Desc Gsheet'
        self.JOB_LOG.info(op)

    def logAddSrcSchemaDescToSS(self, ssID):
        op = ''
        op += '  - Adding schema description to spreadsheet for source ' + ssID
        self.JOB_LOG.info(op)

    def logPopulateSrcTableMapEnd(self):
        op = ''
        op = '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |   Src table map populated  |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    # refreshSchemaDescsFromGsheets

    def logRefreshingSchemaDescsTxtFilesFromGsheetsStart(self):
        op = ''
        op += '\n'
        op += '                 --------------------------------------' + '\n'
        op += '                 | Refresh schema desc txt files      |' + '\n'
        op += '                 |                                    |' + '\n'
        op += '                 |  This will...                      |' + '\n'
        op += '                 |    - Check mod date of scheam desc |' + '\n'
        op += '                 |      GSheets                       |' + '\n'
        op += '                 |    - Extract schemas & save to txt |' + '\n'
        self.JOB_LOG.info(op)

    def logRefreshingSchemaDescsTxtFilesFromGsheetsEnd(self):
        op = ''
        op = '\n'
        op += '                 | Schema desc txt files refereshed   |' + '\n'
        op += '                 --------------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logLoadingDBSchemaDescsFromGsheets(self, dbId):
        op = ''
        op += '    - Extracting schema descriptions for the ' + dbId
        op += ' database...'
        self.JOB_LOG.info(op)

    # Physical Schemas

    def logBuildPhysicalDWHSchemaStart(self):

        op = ''
        op += '\n'
        op += '                 -------------------------------' + '\n'
        op += '                 |  Building Physical Schemas  |' + '\n'
        self.JOB_LOG.info(op)

    def logBuildPhysicalDWHSchemaEnd(self):

        op = ''
        op += '\n'
        op += '                 |   Physical Schemas Built   |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBuildingPhysicalSchema(self, dataLayerID):
        op = ''
        op += '    - Rebuilding the ' + dataLayerID
        op += ' physical data models... '
        self.JOB_LOG.info(op)

    def logExecutionStart(self):

        # introText = 'Running NEW execution'
        # if rerunPrevJob:
        #     introText = 'Rerunning PREVIOUS execution'
        #
        # lastExecStatusMsg = ('The last execution (' +
        #                      str(lastExecReport['lastExecId']) + ') ' +
        #                      'finished with status: ' +
        #                      lastExecReport['lastExecStatus'])

        op = '\n'
        op += '---------------------------------------------------------------'
        op += '--------' + '\n'
        # op += ' ' + introText + ': ' + str(lastExecReport['execId']) + '\n'
        op += '   - Started: ' + str(self.CONF.EXE_START_TIME) + '\n'
        # op += '   - ' + lastExecStatusMsg + '\n'
        op += '---------------------------------------------------------------'
        op += '--------' + '\n'

        self.JOB_LOG.info(op)

    def logExecutionEnd(self):

        currentTime = datetime.now()
        elapsedSecs = (currentTime - self.CONF.EXE_START_TIME).total_seconds()
        elapsedMins = round(elapsedSecs / 60, 1)

        op = '\n'
        op += '                  Finished: ' + str(self.CONF.EXE_START_TIME)
        op += '\n'
        op += '                  Duration: ' + str(elapsedMins) + ' mins'
        op += '\n\n'
        op += '                       ' + self.JOB_LOG_FILE_NAME
        op += '\n'
        self.JOB_LOG.info(op)

    def logExtractStart(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |    Extract Stage: start    |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logExtractEnd(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |     Extract Stage: end     |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logTransformStart(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |   Transform Stage: start   |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logTransformEnd(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |    Transform Stage: end    |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logLoadStart(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |      Load Stage: start     |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBulkLoadSetupStart(self):
        op = ''
        op += '\n'
        op += '                      ------------------------------' + '\n'
        op += '                      |    Bulk load setup: start  |' + '\n'
        op += '                      ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBulkLoadSetupEnd(self):
        op = ''
        op += '\n'
        op += '                      ------------------------------' + '\n'
        op += '                      |     Bulk load setup: end   |' + '\n'
        op += '                      ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logDimLoadStart(self):
        op = ''
        op += '\n'
        op += '                      ------------------------------' + '\n'
        op += '                      |       Dim Load: start      |' + '\n'
        op += '                      ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logDefaultDimLoadStart(self):
        op = ''
        op += '\n'
        op += '                          ------------------------------' + '\n'
        op += '                          |   Default Dim Load: start  |' + '\n'
        op += '                          ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logDefaultDimLoadEnd(self):
        op = ''
        op += '\n'
        op += '                     ------------------------------' + '\n'
        op += '                     |    Default Dim Load: end   |' + '\n'
        op += '                     ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBespokeDimLoadStart(self):
        op = ''
        op += '\n'
        op += '                          ------------------------------' + '\n'
        op += '                          |   Bespoke Dim Load: start  |' + '\n'
        op += '                          ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBespokeDimLoadEnd(self):
        op = ''
        op += '\n'
        op += '                          ------------------------------' + '\n'
        op += '                          |    Bespoke Dim Load: end   |' + '\n'
        op += '                          ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logDimLoadEnd(self):
        op = ''
        op += '\n'
        op += '                      ------------------------------' + '\n'
        op += '                      |        Dim Load: end       |' + '\n'
        op += '                      ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logFactLoadStart(self):
        op = ''
        op += '\n'
        op += '                      ------------------------------' + '\n'
        op += '                      |      Fact Load: start      |' + '\n'
        op += '                      ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logDefaultFactLoadStart(self):
        op = ''
        op += '\n'
        op += '                          ------------------------------' + '\n'
        op += '                          |  Default Fact Load: start  |' + '\n'
        op += '                          ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logDefaultFactLoadEnd(self):
        op = ''
        op += '\n'
        op += '                          ------------------------------' + '\n'
        op += '                          |   Default Fact Load: end   |' + '\n'
        op += '                          ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBespokeFactLoadStart(self):
        op = ''
        op += '\n'
        op += '                          ------------------------------' + '\n'
        op += '                          |   Bespoke Dim Load: start  |' + '\n'
        op += '                          ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBespokeFactLoadEnd(self):
        op = ''
        op += '\n'
        op += '                          ------------------------------' + '\n'
        op += '                          |    Bespoke Dim Load: end   |' + '\n'
        op += '                          ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logFactLoadEnd(self):
        op = ''
        op += '\n'
        op += '                      ------------------------------' + '\n'
        op += '                      |       Fact Load: end       |' + '\n'
        op += '                      ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logLoadEnd(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |       Load Stage: end      |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logDimensionsLoadEnd(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |     Dimension Load: end    |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logSummariseStart(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |   Summarise Stage: start   |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBespokeSummariseStart(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |  Bespoke Summarise: start  |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logBespokeSummariseEnd(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |  Bespoke Summarise: end  |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logSummariseEnd(self):
        op = ''
        op += '\n'
        op += '                 ------------------------------' + '\n'
        op += '                 |    Summarise Stage: end    |' + '\n'
        op += '                 ------------------------------' + '\n'
        self.JOB_LOG.info(op)

    def logInitialiseDatastore(self,
                               datastoreID,
                               datastoreType,
                               isSchemaDesc=False):

        if datastoreID not in ['CTL']:
            desc = '  - Connecting to the ' + datastoreID + ' ' + \
                 datastoreType + ' datastore'
            if isSchemaDesc:
                desc = ('    - Connecting to the ' + datastoreID + ' schema ' +
                        'description spreadsheet')
            op = desc

            if self.JOB_LOG is not None:
                self.JOB_LOG.info(op)
            else:
                print(op)

    def logInitialiseSrcSysDatastore(self, datastoreID, datastoreType):

        op = ''
        op += '\n'
        op += '  - Connecting to source system datastore: ' + datastoreID
        op += ' (' + datastoreType + ')'
        op += '\n'

        self.JOB_LOG.info(op)

    def logRefreshDefaultRowsTxtFileFromGSheetStart(self):
        op = ''
        op += '  - Refreshing the default rows txt file from Google Sheets... '
        self.JOB_LOG.info(op)

    def logRefreshDefaultRowsTxtFileFromGSheetEnd(self):
        op = 'DONE'
        op += '\n'
        self.JOB_LOG.info(op)

    def logDFStart(self, desc, startTime, stage):

        filename = os.path.basename(inspect.stack()[4][1]).replace('.py', '')
        funcname = inspect.stack()[4][3].replace("'", "")

        startStr = startTime.strftime('%H:%M:%S')

        callstack = (stage + ' | ' + filename + '.' + funcname + ' | ' +
                     startStr)
        spacer = ' ' * (62 - len(callstack))
        spacer2 = ' ' * (59 - len(desc))

        op = ''
        op += '\n'
        op += '**************************************************************'
        op += '***\n'
        op += '*                                                             '
        op += '  *\n'
        op += '* ' + callstack + spacer + '*\n'
        op += '*    ' + desc + spacer2 + '*\n'
        op += '*                                                             '
        op += '  *\n'
        op += '**************************************************************'
        op += '***\n'

        self.JOB_LOG.info(op)

    def logDFEnd(self, durationSeconds, df):

        op = ''
        op += '\n[Completed dataflow in: '
        op += str(round(durationSeconds, 2)) + ' seconds] \n\n'

        if df is not None:
            op += self.describeDataFrame(df)

        self.JOB_LOG.info(op)

    def logStepStart(self, startTime, desc, datasetName, df, additionalDesc):

        op = ''
        op = '\n'
        op += '   -------------------------------------------------------\n'
        op += '   | Operation: ' + str(inspect.stack()[3][3]) + '\n'
        if desc is not None:
            op += '   | Desc: "' + desc + '"\n'
        if additionalDesc is not None:
            op += '   | "' + additionalDesc + '"\n'
        if df is not None:
            op += self.describeDataFrame(df, datasetName, isPartOfStepLog=True)
        startStr = startTime.strftime('%H:%M:%S')
        op += '   | [Started step: ' + startStr + ']'

        self.JOB_LOG.info(op)

    def logStepEnd(self, report, duration, datasetName, df, shapeOnly):

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

        self.JOB_LOG.info(op)

    def logStepError(self, str):

        op = ''
        op += '\n'
        op += '\n'
        op += str
        op += '\n'
        op += '\n'

        self.JOB_LOG.info(op)

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
        auditCols = self.CONF.AUDIT_COLS['colNames']
        if set(auditCols).issubset(list(df.columns.values)):
            tableContainsAuditCols = True
            # We should be able to predict the number of audit cols, but that
            # doesn't help much with debugging (which, at this stage, is pretty
            # necessary with the audit functionality).
            numberOfAuditCols = 0
            for col in list(df.columns.values):
                if col in list(auditCols):
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
                if colName not in auditCols.tolist():
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

    def logVariancesReport(self):
        op = ''
        op += '\n'
        op += '*** REPORTS ***'
        self.JOB_LOG.info(op)

    def logNoVariancesReported(self, varianceLimit):
        op = ''
        op += '\n'
        op += 'All step variances for this execution were within \n'
        op += str(varianceLimit) + ' standard deviations of the prior average'
        op += '\n'
        self.JOB_LOG.info(op)

    def logSomeVariancesReported(self, varianceLimit, url):
        op = ''
        op += '\n'
        op += 'Some step variances for this execution were greater than \n'
        op += str(varianceLimit) + ' standard deviations of the prior average.'
        op += ' \n'
        op += 'View the report here: ' + url
        op += '\n'
        self.JOB_LOG.info(op)

    def logAlerts(self, conf):

        # We tag on any alerts to the end of the logs
        op = '\n'
        op += '*** ALERTS ***'
        op += '\n\n'
        alertsText = ''

        alertsFileName = \
            'logs/alerts.txt'
        file = Path(alertsFileName)
        if file.is_file():
            with open(alertsFileName, 'r') as f:
                alertsText = f.read()

        if len(alertsText) == 0:
            alertsText = 'No alerts generated in this execution'

        op += alertsText
        op += '\n'

        self.JOB_LOG.info(op)

# class MemoryUsageThread(threading.Thread):
#     def __init__(self, name='MemoryUsageLogger'):
#         threading.Thread.__init__(self, name=name)
#
#     def run(self):
#         while True:
#             if self.MEMORY_USAGE_LOOP == 'STOP':
#                 # Overwrite the line without creating a lb
#                 print(' ' * 100, end='\r')
#                 self.MEMORY_USAGE_LOOP = 'STOPPED'
#                 sys.exit()
#             else:
#                 sleep(0.1)  # pause for half a second
#                 currentMemUsage = \
#                   psutil.Process(os.getpid()).memory_info().rss
#                 percent = \
#                   round(currentMemUsage / self.MEM_USAGE_AT_START * 100)
#                 op = 'Used Memory: ' + str(percent) + \
#                      '% (as % of use at start' + \
#                      ') (' + datetime.now().strftime("%H:%M:%S") + ')'
#                 print(op, end='\r')
