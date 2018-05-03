import logging
import inspect
from datetime import datetime
import os

JOB_LOG = None

EXEC_ID = None
LOG_LEVEL = logging.ERROR
CONF = None
JOB_LOG_FILE_NAME = None

EXE_START_TIME = None


def initialiseLogging(conf):

    execId = conf.state.EXEC_ID
    logLevel = conf.exe.LOG_LEVEL

    global CONF
    global JOB_LOG
    global EXEC_ID
    global LOG_LEVEL
    global JOB_LOG_FILE_NAME

    CONF = conf
    EXEC_ID = execId
    if logLevel is not None:
        LOG_LEVEL = logLevel

    JOB_LOG_FILE_NAME = 'logs/' + str(EXEC_ID) + '_jobLog.log'

    JOB_LOG = logging.getLogger('JOB_LOG')
    jobLogFileName = JOB_LOG_FILE_NAME
    jobLogFileHandler = logging.FileHandler(jobLogFileName, mode='a')
    streamHandler = logging.StreamHandler()
    JOB_LOG.setLevel(logging.DEBUG)  # Always log everything on this log
    JOB_LOG.addHandler(jobLogFileHandler)
    JOB_LOG.addHandler(streamHandler)

    return logging.getLogger('JOB_LOG')


def getLogger():
    return logging.getLogger('JOB_LOG')


def logExecutionStart(rerun=False):

    global EXE_START_TIME
    EXE_START_TIME = datetime.now()

    value = ''
    if rerun:
        value = 'Restarted'
    else:
        value = 'Started  '

    op = '\n'
    op += '                  *****************************' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *  BETL Execution ' + value + ' *' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *****************************' + '\n'

    JOB_LOG.info(op)


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
    op += '-------' + '\n'
    op += ' ' + introText + ': ' + str(execReport['execId']) + '\n'
    op += '   - Started: ' + str(EXE_START_TIME) + '\n'
    op += '   - ' + lastExecStatusMsg + '\n'
    op += '-----------------------------------------------------------------'
    op += '-------' + '\n'

    JOB_LOG.info(op)


def logInitialiseDatastore(datastoreID, datastoreType, isSchemaDesc=False):

    if datastoreID in ['CTL']:
        pass
    else:
        desc = 'Connecting to the ' + datastoreID + ' ' + \
             datastoreType + ' datastore'
        if isSchemaDesc:
            desc = 'Connecting to the ' + datastoreID + ' schema ' + \
                'description spreadsheet'
        op = ''
        op += desc

        if JOB_LOG is not None:
            JOB_LOG.info(op)
        else:
            print(op)


def logInitialiseSrcSysDatastore(datastoreID, datastoreType):

    op = ''
    op += '   *** Connecting to source system datastore: ' + datastoreID
    op += ' (' + datastoreType + ') ***'
    op += '\n'

    JOB_LOG.info(op)


def logDFStart(desc, startTime):

    stage = CONF.state.STAGE
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

    op = ''
    op += '   -------------------------------------------------------\n'
    op += '   | Operation: ' + str(inspect.stack()[1][3]) + '\n'
    if desc is not None:
        op += '   | Desc: "' + desc + '"\n'
    if additionalDesc is not None:
        op += '   | "' + additionalDesc + '"\n'
    if df is not None:
        op += describeDataFrame(df, datasetName, isPartOfStepLog=True)
    startStr = startTime.strftime('%H:%M:%S')
    op += '   | [Started step: ' + startStr + ']'

    JOB_LOG.info(op)


def logStepEnd(report, duration, datasetName=None, df=None, shapeOnly=False):
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
    if set(CONF.auditColumns['colNames']).issubset(list(df.columns.values)):
        tableContainsAuditCols = True
        numberOfColumns = numberOfColumns - len(CONF.auditColumns['colNames'])

    op = ''
    op += '   ' + firstChar + 'Output: ' + str(df.shape[0]) + ' rows, '
    op += str(numberOfColumns) + ' cols'
    if tableContainsAuditCols:
        op += ' (& audit cols)'
    if datasetName is not None:
        op += ' [' + datasetName + ']'
    op += '\n'
    if not shapeOnly:
        op += '   ' + firstChar + 'Columns:\n'
        for colName in list(df.columns.values):
            if colName not in CONF.auditColumns['colNames'].tolist():
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


def logExecutionFinish(logStr):

    op = ''
    op += logStr
    op += '\n'
    op += '                  *****************************' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *  BETL Execution Finished  *' + '\n'
    op += '                  *                           *' + '\n'
    op += '                  *****************************' + '\n'

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

    JOB_LOG.info(op)


def logBetlSetupComplete():

    op = ''
    op += '\n'
    op += '-------------------------' + '\n'
    op += ' BETL setup successfully ' + '\n'
    op += '-------------------------' + '\n'

    JOB_LOG.info(op)


def logClearedTempData():

    op = ''
    op += '\n\n'
    op += '-----------------------' + '\n'
    op += ' Cleared all temp data ' + '\n'
    op += '-----------------------' + '\n'

    JOB_LOG.info(op)


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


def logRefreshingSchemaDescsFromGsheets(dbCount):
    op = ''
    op += '\n'
    op += '*** Refreshing the schema descriptions for ' + str(dbCount) + ' '
    op += 'databases from Google Sheets ***'
    op += '\n'
    JOB_LOG.info(op)


def logLoadingDBSchemaDescsFromGsheets(dbID):
    op = ''
    op += 'Loading schema descriptions for the ' + dbID + ' database...'
    JOB_LOG.info(op)


def logRefreshingSchemaDescsFromGsheets_done():
    op = ''
    op += '\n'
    JOB_LOG.info(op)


def logSchemaDescsLoad():
    op = ''
    op += '*** Loading the schema descriptions ***'
    op += '\n'
    JOB_LOG.info(op)

    # if logicalDataModels is not None:
    #     for dmID in logicalDataModels:
    #         op += logicalDataModels[dmID].__str__()


def logPhysicalDataModelBuild():
    op = ''
    op += '*** Rebuilding the physical data models ***'
    op += '\n'
    JOB_LOG.info(op)


def logRebuildingPhysicalDataModel(dataLayerID):
    op = ''
    op += '  - Rebuilding the ' + dataLayerID + ' physical data models... '
    JOB_LOG.info(op)


def superflousTableWarning(tableNamesStr):
    op = ''
    op += '\n'
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
