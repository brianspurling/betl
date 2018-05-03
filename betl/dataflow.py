import pandas as pd
import os
from datetime import datetime

from . import logger
from . import fileIO
from . import dbIO
from . import gsheetIO


# TODO: need to go through and think carefully about copy vs views
# on DF. For starters, probably get rid of the df = at the start of every
# dataflow!
class DataFlow():

    def __init__(self, conf, desc):

        self.startTime = datetime.now()
        self.conf = conf
        self.description = desc
        self.data = {}
        self.trgDataset = None
        # trgDataset is always set to the most recent dataset written to disk

        logger.logDFStart(self.description, self.startTime)

    def close(self, df=None):
        elapsedSeconds = (datetime.now() - self.startTime).total_seconds()
        logger.logDFEnd(elapsedSeconds, self.trgDataset)

        for dataset in self.data:
            del(self.data[dataset]['df'])
        del(self.trgDataset)

    def getDataFromSrc(self, tableName, srcSysID, desc=None):

        srcSysDatastore = self.conf.data.getSrcSysDatastore(srcSysID)

        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        limitdata = self.conf.exe.DATA_LIMIT_ROWS

        df = pd.DataFrame()

        if srcSysDatastore.datastoreType == 'FILESYSTEM':
            filename = tableName
            path = srcSysDatastore.path
            separator = srcSysDatastore.delim
            quotechar = srcSysDatastore.quotechar

            if srcSysDatastore.fileExt == '.csv':
                df = fileIO.readDataFromCsv(conf=self.conf,
                                            path=path,
                                            filename=filename + '.csv',
                                            sep=separator,
                                            quotechar=quotechar,
                                            isTmpData=False,
                                            limitdata=limitdata)

            else:
                raise ValueError('Unhandled file extension for src system: ' +
                                 srcSysDatastore.fileExt + ' for source sys ' +
                                 srcSysID)

        elif srcSysDatastore.datastoreType in ('POSTGRES', 'SQLITE'):

            etlTableName = tableName
            # Cut off the src_<dataModelID>_ prefix, by doing
            # two "left trims" on the "_" char
            srcTableName = etlTableName[etlTableName.find("_")+1:]
            srcTableName = srcTableName[srcTableName.find("_")+1:]

            df = dbIO.readDataFromDB(tableName=srcTableName,
                                     conn=srcSysDatastore.conn,
                                     cols='*',
                                     limitdata=limitdata)

        elif srcSysDatastore.datastoreType == 'SPREADSHEET':
            df = gsheetIO.readDataFromWorksheet(
                worksheet=srcSysDatastore.worksheets[tableName],
                limitdata=limitdata)

        else:
            raise ValueError('Extract for source systems type <'
                             + srcSysDatastore.datastoreType
                             + '> connection type not supported')

        self.data[tableName] = {
            'name': tableName,
            'dataLayer': None,
            'df': df}

        report = 'Read ' + str(df.shape[0]) + ' rows from source: ' + tableName
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, tableName, df)

    def read(self,
             tableName,
             dataLayer,
             targetDataset=None,
             forceDBRead=False,
             desc=None):

        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        _targetDataset = tableName
        if targetDataset is not None:
            _targetDataset = targetDataset

        if _targetDataset in self.data:
            raise ValueError('There is already a dataset named ' +
                             _targetDataset + ' in this dataflow')

        path = (self.conf.ctrl.TMP_DATA_PATH + dataLayer + '/')
        filename = tableName + '.csv'

        df = pd.DataFrame()
        if forceDBRead:
            df = dbIO.readDataFromDB(
                tableName=tableName,
                # TODO: Can't do this! dbs labelled by db id, not datalayer
                conn=self.conf.data.getDatastore(dataLayer).conn)

        else:
            df = fileIO.readDataFromCsv(conf=self.conf,
                                        path=path,
                                        filename=filename,
                                        sep=',',
                                        quotechar='"')

        self.data[_targetDataset] = {
            'name': _targetDataset,
            'dataLayer': dataLayer,
            'df': df}

        shape = df.shape
        report = 'Read (' + str(shape[0]) + ', ' + str(shape[1]) + ') ' + \
                 'from ' + tableName
        if targetDataset is not None:
            report += ' and saved to ' + _targetDataset
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, _targetDataset, df)

    def createDataset(self, dataset, data, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        # data can be a dictionary of columnName:value, where value is
        # hardcoded or an array. Or data can be a pandas dataframe.
        if str(type(data)) == 'pandas.core.frame.DataFrame':
            df = data
        else:
            df = pd.DataFrame(columns=list(data.keys()))

        for col in data:
            df[col] = data[col]

        self.data[dataset] = {
            'name': dataset,
            'dataLayer': None,
            'df': df}

        report = 'Created ' + dataset + ' table with ' + str(df.shape[0]) + \
                 ' rows'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df)

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

        startTime = datetime.now()
        self.trgDataset = self.data[dataset]['df']
        logger.logStepStart(startTime, desc, dataset, self.trgDataset)

        # Work out whether we need to write to DB as well as CSV
        writeToDB = False

        if dataLayerID in ['TRG', 'SUM']:
            writeToDB = True
        elif forceDBWrite:
            writeToDB = True
        else:
            writeToDB = self.conf.exe.WRITE_TO_ETL_DB

        dataLayer = self.conf.getLogicalDataModel(dataLayerID)
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
                self.conf.auditColumns['colNames'].tolist()
            colsIncludeSKs = False
            colsIncludeAudit = False
            for colName in list(self.trgDataset):
                if colName in logDataModelColNames_sks:
                    colsIncludeSKs = True
                if colName in self.conf.auditColumns['colNames'].tolist():
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
                        self.conf.auditColumns['colNames'].tolist()
                if not colsIncludeSKs and not colsIncludeAudit:
                    colsToSortBy = logDataModelColNames_noSKs
                self.trgDataset = self.trgDataset[colsToSortBy]

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
                self.trgDataset,
                targetTableName,
                dbEng,
                append_or_replace)

        # write to CSV
        mode = 'w'
        if append_or_replace.upper() == 'APPEND':
            mode = 'a'

        if writingDefaultRows:
            self.trgDataset.drop(
                self.trgDataset.columns[0],
                axis=1,
                inplace=True)

        path = (self.conf.ctrl.TMP_DATA_PATH + dataLayerID + '/')
        if not os.path.exists(path):
            os.makedirs(path)

        filename = targetTableName + '.csv'

        fileIO.writeDataToCsv(
            conf=self.conf,
            df=self.trgDataset,
            path=path,
            filename=filename,
            headers=True,
            mode=mode)

        report = str(self.trgDataset.shape[0]) + ' rows written to '
        report += targetTableName
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

        if not keepDataflowOpen:
            self.close()

    def dropColumns(self,
                    dataset,
                    colsToDrop=None,
                    colsToKeep=None,
                    desc=None,
                    dropAuditCols=False):

        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        if colsToDrop is not None and colsToKeep is not None:
            raise ValueError("Nope!")

        auditCols = self.conf.auditColumns['colNames'].tolist()
        if colsToKeep is not None:
            colsToKeep = colsToKeep + auditCols
            colsToDrop = [col for col in list(df) if col not in colsToKeep]

        if dropAuditCols and set(auditCols).issubset(list(df)):
            colsToDrop += auditCols

        df.drop(
            colsToDrop,
            axis=1,
            inplace=True)

        report = 'Dropped ' + str(len(colsToDrop)) + ' columns'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df)

    def renameColumns(self, dataset, columns, desc=None):

        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        df.rename(index=str,
                  columns=columns,
                  inplace=True)

        report = 'Renamed ' + str(len(columns)) + ' columns'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df)

    def addColumns(self, dataset, columns, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        # columns is a dictionary of columnName:value pairs
        # The value can be a hard-coded value or a series. Both will work
        # with a simple pandas assigment. The value can also be a function.
        # If so, it needs to be run on the dataframe with the apply() func

        for col in columns:
            if callable(columns[col]):
                df[col] = df.apply(columns[col], axis=1)
            else:
                df[col] = columns[col]

        # If the dataset has audit cols, then we just added after them,
        # so sort the audit cols back to the end
        auditColList = self.conf.auditColumns['colNames'].tolist()
        colList = list(df.columns.values)
        isAuditColsInDataset = \
            set(auditColList).issubset(list(df.columns.values))
        if isAuditColsInDataset:
            colList = [col for col in list(df.columns.values)
                       if col not in auditColList]
            colList = list(df.columns.values) + auditColList
        df = df[colList]

        report = 'Assigned ' + str(len(columns)) + ' to dataset'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df)

    def setColumns(self, dataset, columns, desc=None):
        # A wrapper for semantic purposes
        self.addColumns(dataset, columns, desc)

    def setNulls(self, dataset, columns, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        for col in columns:
            df.loc[df[col].isnull(), col] = columns[col]

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

    def filter(self, dataset, filters, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']
        originalLength = df.shape[0]

        for f in filters:
            if isinstance(filters[f], str):
                df = df.loc[df[f] == filters[f]]
            elif isinstance(filters[f], tuple):
                if filters[f][0] == '>':
                    df = df.loc[df[f] > filters[f][1]]
                elif filters[f][0] == '<':
                    df = df.loc[df[f] > filters[f][1]]
                elif filters[f][0] == '==':
                    df = df.loc[df[f] == filters[f][1]]
                else:
                    raise ValueError(
                        'Filter currently only support ==, < or >')
            else:
                raise ValueError('filter value must be str or tuple (not ' +
                                 str(type(filters[f])) + ')')

        newLength = df.shape[0]
        pcntChange = (originalLength - newLength) / originalLength
        pcntChange = round(pcntChange * 100, 1)
        report = 'Filtered dataset from ' + str(originalLength) + ' to ' + \
                 str(newLength) + ' rows (' + str(pcntChange) + ')'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df, shapeOnly=True)

    def cleanColumn(self,
                    dataset,
                    cleaningFunc,
                    column,
                    cleanedColumn=None,
                    desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        if cleanedColumn is None:
            cleanedColumn = column

        self.data[dataset]['df'][cleanedColumn] = \
            cleaningFunc(self.data[dataset]['df'][column])

        # If the dataset has audit cols, then we just added after them,
        # so sort the audit cols back to the end
        auditColList = self.conf.auditColumns['colNames'].tolist()
        colList = list(self.data[dataset]['df'])
        isAuditColsInDataset = \
            set(auditColList).issubset(list(self.data[dataset]['df']))
        if isAuditColsInDataset:
            colList = [col for col in list(self.data[dataset]['df'])
                       if col not in auditColList]
            colList = list(self.data[dataset]['df']) + auditColList
        df_reordered = self.data[dataset]['df'][colList]

        report = 'Cleaned ' + column + ' to ' + cleanedColumn
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df_reordered)

    def union(self, datasets, targetDataset, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        dfsToConcat = []
        for dataset in datasets:
            dfsToConcat.append(self.data[dataset]['df'])
        df = pd.concat(dfsToConcat)
        self.data[targetDataset] = {
            'name': targetDataset,
            'dataLayer': None,
            'df': df}

        report = 'Concatenated ' + str(len(dfsToConcat)) + \
                 ' dfs, totalling ' + str(df.shape[0]) + ' rows'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df)

    def getColumns(self, dataset, columnNames, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        if isinstance(columnNames, str):
            cols = df[columnNames]
        elif isinstance(columnNames, list):
            cols = {}
            for columnName in columnNames:
                cols[columnName] = df[columnName]
        else:
            raise ValueError('columnNames must be string or list')

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

        return cols

    def getDataFrames(self, datasets, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        if isinstance(datasets, str):
            dfs = self.data[datasets]['df']
        elif isinstance(datasets, list):
            dfs = {}
            for dataset in datasets:
                dfs[dataset] = datasets[dataset]['df']
        else:
            raise ValueError('datasets must be string or list')

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

        return dfs

    def sortColumns(self, dataset, colList, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        # If the dataset has audit cols, then we just added after them,
        # so sort the audit cols back to the end
        auditColList = self.conf.auditColumns['colNames'].tolist()
        isAuditColsInDataset = \
            set(auditColList).issubset(list(df))
        if isAuditColsInDataset:
            colList = [col for col in colList if col not in auditColList]
            colList = list(df.columns.values) + auditColList

        if set(colList) != set(list(df.columns.values)):
            raise ValueError('You have attempted to sort columns without ' +
                             'providing a list of columns that exactly ' +
                             'matches the dataset. Dataset cols: ' +
                             str(list(df)) + '. sortColList: ' + str(colList))

        df_reordered = df[colList]

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df_reordered)

    def duplicateDataset(self, dataset, targetDatasets, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        for targetDataset in targetDatasets:
            self.data[targetDataset] = {
                'name': targetDataset,
                'dataLayer': None,
                'df': df.copy()}

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

    def dedupe(self, dataset, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        # TODO: this was first written for deduping the posts load to ODS,
        # where I think we can expect all audit cols to have the same values
        # (maybe not - not really thought about deltas in detail). But what
        # happens if using this in another instance, where you're actually
        # raising the grain. Need to deal with audit cols...
        df.drop_duplicates(inplace=True)

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df)

    def join(self,
             datasets,
             targetDataset,
             joinCol,
             how,
             keepCols=None,
             desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        if len(datasets) > 2:
            raise ValueError('You can only join two tables at once')

        df1 = self.data[datasets[0]]['df']
        df2 = self.data[datasets[1]]['df']
        # TODO: this was initially written for dm_audit, which doesn't need
        # it's own audit columns. But if it's used for a normal table, we
        # will need to accommodate audit columns in this merge operation
        df = pd.merge(df1, df2, on=joinCol, how=how)
        if keepCols is not None:
            df = df[keepCols]

        self.data[targetDataset] = {
            'name': targetDataset,
            'dataLayer': None,
            'df': df}

        report = 'Joined datasets ' + datasets[0] + ' (' + \
                 str(df1.shape[0]) + ' rows) and ' + datasets[1] + ' (' + \
                 str(df2.shape[0]) + ' rows) into ' + targetDataset + ' (' + \
                 str(df.shape[0]) + ' rows)'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, targetDataset, df)

    def setAuditCols(self, dataset, bulkOrDelta, sourceSystem, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        if bulkOrDelta == 'BULK':
            df['audit_source_system'] = sourceSystem
            df['audit_bulk_load_date'] = datetime.now()
            df['audit_latest_delta_load_date'] = None
            df['audit_latest_load_operation'] = 'BULK'

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

    def createAuditNKs(self, dataset, desc):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        # TODO will improve this over time. Basic solution as PoC
        df['nk_audit'] = \
            df['audit_latest_load_operation'] + '_' + '10'

        df.drop(
            ['audit_source_system',
             'audit_bulk_load_date',
             'audit_latest_delta_load_date',
             'audit_latest_load_operation'],
            axis=1,
            inplace=True)

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

    def getColumnList(self, dataset, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        colList = list(df)

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

        return colList

    def customSQL(self, sql, dataLayer, dataset=None, desc=''):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc, additionalDesc=sql)

        datastore = self.conf.getLogicalDataModel(dataLayer).datastore
        df = dbIO.customSQL(sql, datastore)

        if dataset is not None:
            self.data[dataset] = {
                'name': dataset,
                'dataLayer': None,
                'df': df}

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df)

    def iterate(self, dataset, function, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']

        for row in df.itertuples():
            function(row)

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

    def truncate(self, dataset, dataLayerID, forceDBWrite=False, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        path = (self.conf.ctrl.TMP_DATA_PATH + dataLayerID + '/')
        filename = dataset + '.csv'

        fileIO.truncateFile(self.conf, path, filename)

        if forceDBWrite:
            dataLayer = self.conf.getLogicalDataModel(dataLayerID)
            dbIO.truncateTable(dataset, dataLayer.datastore)

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

    def templateStep(self, dataset, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        df = self.data[dataset]['df']
        df['temp'] = 'temp'
        # df.<Command here>

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df)

    def __str__(self):
        op = ''
        op += 'DataFlow: ' + self.description + '\n'
        op += '  Source data: \n'
        for src in self.data:
            op += '    ' + self.data[src]['name']
            op += ' (' + self.data[src]['dataLayer'] + '): '
            op += str(self.data[src]['df'].shape)
            op += '\n'
        op += '\n'
        return op

# logger.logClearedTempData()
