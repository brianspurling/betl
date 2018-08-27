import pandas as pd
import os
from betl.io import dbIO
from betl.io import fileIO
from betl.io import gsheetIO
from betl.io import excelIO


def read(self,
         tableName,
         dataLayer,
         targetDataset=None,
         forceDBRead=False,
         desc=None):

    if desc is None:
        desc = 'Read data from ' + dataLayer + '.' + tableName + \
               ' (forceDBRead = ' + str(forceDBRead) + ')'
    self.stepStart(desc=desc)

    _targetDataset = tableName
    if targetDataset is not None:
        _targetDataset = targetDataset

    if _targetDataset in self.data:
        raise ValueError('There is already a dataset named ' +
                         _targetDataset + ' in this dataflow')

    path = (self.CONF.CTRL.TMP_DATA_PATH + '/' + dataLayer + '/')
    filename = tableName + '.csv'

    self.data[_targetDataset] = pd.DataFrame()

    if forceDBRead:
        dbID = \
            self.CONF.DATA.getDataLayerLogicalSchema(dataLayer).databaseID
        self.data[_targetDataset] = dbIO.readDataFromDB(
            tableName=tableName,
            conn=self.CONF.DATA.getDWHDatastore(dbID).conn)

    else:
        fileNameMap = self.CONF.STATE.FILE_NAME_MAP
        self.data[_targetDataset] = \
            fileIO.readDataFromCsv(fileNameMap=fileNameMap,
                                   path=path,
                                   filename=filename,
                                   sep=',',
                                   quotechar='"')

    shape = self.data[_targetDataset].shape
    report = 'Read (' + str(shape[0]) + ', ' + str(shape[1]) + ') ' + \
             'from ' + tableName
    if targetDataset is not None:
        report += ' and saved to ' + _targetDataset

    self.stepEnd(
        report=report,
        datasetName=_targetDataset,
        df=self.data[_targetDataset])


def write(self,
          dataset,
          targetTableName,
          dataLayerID,
          forceDBWrite=False,
          dtype=None,
          append_or_replace='replace',
          writingDefaultRows=False,
          desc=None,
          keepDataflowOpen=False):

    if desc is None:
        desc = 'Write data to ' + dataLayerID + '.' + targetTableName + \
               ' (append_or_replace = ' + append_or_replace + ';' + \
               ' writingDefaultRows = ' + str(writingDefaultRows) + ';' + \
               ' forceDBWrite = ' + str(forceDBWrite) + ')'
    self.stepStart(desc=desc, datasetName=dataset)

    self.targetDataset = self.data[dataset]

    # Work out whether we need to write to DB as well as CSV
    writeToDB = False

    if dataLayerID in ['BSE', 'SUM']:
        writeToDB = True
    elif forceDBWrite:
        writeToDB = True
    else:
        writeToDB = self.CONF.EXE.WRITE_TO_ETL_DB

    dataLayer = self.CONF.DATA.getDataLayerLogicalSchema(dataLayerID)
    if (targetTableName not in dataLayer.getListOfTables()
       and not forceDBWrite):
        writeToDB = False

    # We need to check manually for isSrcSys and raise an error if we're
    # trying to write to a srcSys. PostGres and SqlLite also push this
    # check down to the connection itself, but this doesn't apply to csv
    # or spreadsheet, and nor does it cover sqlalchemy's engine (used by
    # Pandas). Hence the check here.

    if dataLayer.datastore.isSrcSys:
        raise ValueError("You just attempted to write to a source system!")

    # If this is a table defined in our logical data model, then we
    # can check we have all the columns and reorder them to match
    # the schema (which saves the app having to worry about this)
    logDataModelCols = dataLayer.getColumnsForTable(targetTableName)

    if logDataModelCols is not None:

        logDataModelColNames_sks = []
        logDataModelColNames_all = []
        logDataModelColNames_noSKs = []
        logDataModelColNames_all_plus_audit = []

        for col in logDataModelCols:
            logDataModelColNames_all.append(col.columnName)
            if not col.isSK:
                logDataModelColNames_noSKs.append(col.columnName)
            else:
                logDataModelColNames_sks.append(col.columnName)
        logDataModelColNames_all_plus_audit = \
            logDataModelColNames_all + \
            self.CONF.DATA.AUDIT_COLS['colNames'].tolist()
        colsIncludeSKs = False
        colsIncludeAudit = False
        for colName in list(self.targetDataset):
            if colName in logDataModelColNames_sks:
                colsIncludeSKs = True
            if colName in self.CONF.DATA.AUDIT_COLS['colNames'].tolist():
                colsIncludeAudit = True

            if colName not in logDataModelColNames_all_plus_audit:
                raise ValueError(
                    "You're trying to write data to a table that's been " +
                    "defined in your logical data model, but the " +
                    "columns in your dataset don't match the logical " +
                    "data model. Column '" + colName +
                    "' is not in the logical data model.")
        try:
            colsToSortBy = None
            if colsIncludeSKs and colsIncludeAudit:
                colsToSortBy = logDataModelColNames_all_plus_audit
            if colsIncludeSKs and not colsIncludeAudit:
                colsToSortBy = logDataModelColNames_all
            if not colsIncludeSKs and colsIncludeAudit:
                colsToSortBy = logDataModelColNames_noSKs + \
                    self.CONF.DATA.AUDIT_COLS['colNames'].tolist()
            if not colsIncludeSKs and not colsIncludeAudit:
                colsToSortBy = logDataModelColNames_noSKs
            self.targetDataset = self.targetDataset[colsToSortBy]

        except KeyError as e:
            raise ValueError(
                "You're trying to write data to a table that's been " +
                "defined in your logical data model, but the " +
                "columns in your dataset don't match the logical " +
                "data model. Your dataset is missing one or more " +
                "columns: '" + str(e))

    # write to DB
    if writeToDB:
        dbEng = dataLayer.datastore.eng
        dbIO.writeDataToDB(
            self.targetDataset,
            targetTableName,
            dbEng,
            append_or_replace)

    # write to CSV
    mode = 'w'
    if append_or_replace.upper() == 'APPEND':
        mode = 'a'

    if writingDefaultRows:
        self.targetDataset.drop(
            self.targetDataset.columns[0],
            axis=1,
            inplace=True)

    path = (self.CONF.CTRL.TMP_DATA_PATH + '/' + dataLayerID + '/')
    if not os.path.exists(path):
        os.makedirs(path)

    filename = targetTableName + '.csv'

    fileIO.writeDataToCsv(
        conf=self.CONF,
        df=self.targetDataset,
        path=path,
        filename=filename,
        headers=True,
        mode=mode)

    report = str(self.targetDataset.shape[0]) + ' rows written to '
    report += targetTableName

    self.stepEnd(report=report, df=self.targetDataset)
    if not keepDataflowOpen:
        self.close()


def getDataFromSrc(self, tableName, srcSysID, desc, mappedTableName=None):

    self.stepStart(desc=desc)

    srcSysDatastore = self.CONF.DATA.getSrcSysDatastore(srcSysID)

    limitdata = self.CONF.EXE.DATA_LIMIT_ROWS

    self.data[tableName] = pd.DataFrame()

    if mappedTableName is not None:
        srcTableName = mappedTableName
    else:
        # Cut off the src_<datasetID>_ prefix, by doing
        # two "left trims" on the "_" char
        srcTableName = tableName[tableName.find("_")+1:]
        srcTableName = srcTableName[srcTableName.find("_")+1:]

    if srcSysDatastore.datastoreType == 'FILESYSTEM':

        path = srcSysDatastore.path
        separator = srcSysDatastore.delim
        quotechar = srcSysDatastore.quotechar

        if srcSysDatastore.fileExt == '.csv':
            fileNameMap = self.CONF.STATE.FILE_NAME_MAP
            self.data[tableName] = \
                fileIO.readDataFromCsv(fileNameMap=fileNameMap,
                                       path=path,
                                       filename=srcTableName + '.csv',
                                       sep=separator,
                                       quotechar=quotechar,
                                       isTmpData=False,
                                       limitdata=limitdata)

        else:
            raise ValueError('Unhandled file extension for src system: ' +
                             srcSysDatastore.fileExt + ' for source sys ' +
                             srcSysID)

    elif srcSysDatastore.datastoreType in ('POSTGRES', 'SQLITE'):

        self.data[tableName] = \
            dbIO.readDataFromDB(tableName=srcTableName,
                                conn=srcSysDatastore.conn,
                                cols='*',
                                limitdata=limitdata)

    elif srcSysDatastore.datastoreType == 'GSHEET':

        self.data[tableName] = \
            gsheetIO.readDataFromWorksheet(
                worksheet=srcSysDatastore.worksheets[srcTableName],
                limitdata=limitdata)

    elif srcSysDatastore.datastoreType == 'EXCEL':

        self.data[tableName] = \
            excelIO.readDataFromWorksheet(
                worksheet=srcSysDatastore.worksheets[srcTableName],
                limitdata=limitdata)

    else:
        raise ValueError('Extract for source systems type <'
                         + srcSysDatastore.datastoreType
                         + '> connection type not supported')

    report = 'Read ' + str(self.data[tableName].shape[0])
    report += ' rows from source: ' + tableName

    self.stepEnd(
        report=report,
        datasetName=tableName,
        df=self.data[tableName])


def createDataset(self, dataset, data, desc):

    self.stepStart(desc=desc)

    # data can be a dictionary of columnName:value, where value is
    # hardcoded or an array. Or data can be a pandas dataframe.
    if str(type(data)) == 'pandas.core.frame.DataFrame':
        self.data[dataset] = data
    else:
        self.data[dataset] = pd.DataFrame(columns=list(data.keys()))

    for col in data:
        self.data[dataset][col] = data[col]

    report = 'Created ' + dataset + ' table with '
    report += str(self.data[dataset].shape[0]) + ' rows'

    self.stepEnd(
        report=report,
        datasetName=dataset,
        df=self.data[dataset])


def duplicateDataset(self, dataset, targetDatasets, desc):

    self.stepStart(desc=desc)

    if isinstance(targetDatasets, str):
        targetDatasets = [targetDatasets]

    for targetDataset in targetDatasets:
        self.data[targetDataset] = self.data[dataset].copy()

    report = ''

    self.stepEnd(report=report)


def getDataFrames(self, datasets, desc=None):

    if desc is None:
        desc = 'get dataframes: ' + str(datasets)

    self.stepStart(desc=desc)

    if isinstance(datasets, str):
        dfs = self.data[datasets].copy()
    elif isinstance(datasets, list):
        dfs = {}
        for dataset in datasets:
            dfs[dataset] = datasets[dataset].copy()
    else:
        raise ValueError('datasets must be string or list')

    report = ''

    self.stepEnd(report=report)

    return dfs


def getColumns(self, dataset, columnNames, desc=None):

    if desc is None:
        desc = 'Get columns for ' + dataset

    self.stepStart(desc=desc)

    if isinstance(columnNames, str):
        cols = self.data[dataset][columnNames]
    elif isinstance(columnNames, list):
        cols = {}
        for columnName in columnNames:
            cols[columnName] = self.data[dataset][columnName]
    else:
        raise ValueError('columnNames must be string or list')

    report = ''

    self.stepEnd(report=report)

    return cols


def getColumnList(self, dataset, desc=None):

    if desc is None:
        desc = 'Get column list for ' + dataset

    self.stepStart(desc=desc)

    colList = list(self.data[dataset])

    report = ''

    self.stepEnd(report=report)

    return colList
