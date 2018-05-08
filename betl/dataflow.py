import pandas as pd
import os
import pprint
from datetime import datetime

from . import logger
from . import fileIO
from . import dbIO
from . import gsheetIO


class DataFlow():

    def __init__(self, conf, desc):

        self.startTime = datetime.now()
        self.conf = conf
        self.description = desc
        self.data = {}
        # trgDataset is always set to the most recent dataset written to disk
        self.trgDataset = None

        logger.logDFStart(self.description, self.startTime)

    def close(self, df=None):

        elapsedSeconds = (datetime.now() - self.startTime).total_seconds()
        logger.logDFEnd(elapsedSeconds, self.trgDataset)

        # By removing all keys, we remove all pointers to the dataframes,
        # hence making them available to garbage collection
        self.data.clear()
        del(self.trgDataset)

    def getDataFromSrc(self, tableName, srcSysID, desc=None):

        srcSysDatastore = self.conf.data.getSrcSysDatastore(srcSysID)

        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        limitdata = self.conf.exe.DATA_LIMIT_ROWS

        self.data[tableName] = pd.DataFrame()

        if srcSysDatastore.datastoreType == 'FILESYSTEM':
            filename = tableName
            path = srcSysDatastore.path
            separator = srcSysDatastore.delim
            quotechar = srcSysDatastore.quotechar

            if srcSysDatastore.fileExt == '.csv':
                self.data[tableName] = \
                    fileIO.readDataFromCsv(conf=self.conf,
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

            self.data[tableName] = \
                dbIO.readDataFromDB(tableName=srcTableName,
                                    conn=srcSysDatastore.conn,
                                    cols='*',
                                    limitdata=limitdata)

        elif srcSysDatastore.datastoreType == 'SPREADSHEET':
            self.data[tableName] = \
                gsheetIO.readDataFromWorksheet(
                    worksheet=srcSysDatastore.worksheets[tableName],
                    limitdata=limitdata)

        else:
            raise ValueError('Extract for source systems type <'
                             + srcSysDatastore.datastoreType
                             + '> connection type not supported')

        report = 'Read ' + str(self.data[tableName].shape[0])
        report += ' rows from source: ' + tableName

        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(
            report,
            elapsedSeconds,
            tableName,
            self.data[tableName])

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

        self.data[_targetDataset] = pd.DataFrame()

        if forceDBRead:
            dbID = self.conf.LOGICAL_DATA_MODELS[dataLayer].databaseID
            self.data[_targetDataset] = dbIO.readDataFromDB(
                tableName=tableName,
                conn=self.conf.data.getDatastore(dbID).conn)

        else:
            self.data[_targetDataset] = \
                fileIO.readDataFromCsv(conf=self.conf,
                                       path=path,
                                       filename=filename,
                                       sep=',',
                                       quotechar='"')

        shape = self.data[_targetDataset].shape
        report = 'Read (' + str(shape[0]) + ', ' + str(shape[1]) + ') ' + \
                 'from ' + tableName
        if targetDataset is not None:
            report += ' and saved to ' + _targetDataset
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(
            report,
            elapsedSeconds,
            _targetDataset,
            self.data[_targetDataset])

    def createDataset(self, dataset, data, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

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
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, self.data[dataset])

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
        self.trgDataset = self.data[dataset]
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

        if colsToDrop is not None and colsToKeep is not None:
            raise ValueError("Nope!")

        auditCols = self.conf.auditColumns['colNames'].tolist()
        if colsToKeep is not None:
            colsToKeep = colsToKeep + auditCols
            colsToDrop = [col for col in list(self.data[dataset])
                          if col not in colsToKeep]

        if dropAuditCols and set(auditCols).issubset(list(self.data[dataset])):
            colsToDrop += auditCols

        self.data[dataset].drop(
            colsToDrop,
            axis=1,
            inplace=True)

        report = 'Dropped ' + str(len(colsToDrop)) + ' columns'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, self.data[dataset])

    def renameColumns(self, dataset, columns, desc=None):

        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        self.data[dataset].rename(index=str,
                                  columns=columns,
                                  inplace=True)

        report = 'Renamed ' + str(len(columns)) + ' columns'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, self.data[dataset])

    def addColumns(self, dataset, columns, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        # columns is a dictionary of columnName:value pairs
        # The value can be a hard-coded value or a series. Both will work
        # with a simple pandas assigment. The value can also be a function.
        # If so, it needs to be run on the dataframe with the apply() func

        for col in columns:
            if callable(columns[col]):
                self.data[dataset][col] = \
                    self.data[dataset].apply(columns[col], axis=1)
            else:
                self.data[dataset][col] = columns[col]

        report = 'Assigned ' + str(len(columns)) + ' to dataset'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, self.data[dataset])

    def setColumns(self, dataset, columns, desc=None):
        # A wrapper for semantic purposes
        self.addColumns(dataset, columns, desc)

    def setNulls(self, dataset, columns, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        for col in columns:
            self.data[dataset].loc[self.data[dataset][col].isnull(), col] = \
                columns[col]

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

    def filter(self, dataset, filters, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)
        originalLength = self.data[dataset].shape[0]

        for f in filters:
            if isinstance(filters[f], str):
                self.data[dataset] = \
                    self.data[dataset].loc[
                        self.data[dataset][f] == filters[f]]
            elif isinstance(filters[f], tuple):
                if filters[f][0] == '>':
                    self.data[dataset] = \
                        self.data[dataset].loc[
                            self.data[dataset][f] > filters[f][1]]
                elif filters[f][0] == '<':
                    self.data[dataset] = \
                        self.data[dataset].loc[
                            self.data[dataset][f] > filters[f][1]]
                elif filters[f][0] == '==':
                    self.data[dataset] = \
                        self.data[dataset].loc[
                            self.data[dataset][f] == filters[f][1]]
                else:
                    raise ValueError(
                        'Filter currently only support ==, < or >')
            else:
                raise ValueError('filter value must be str or tuple (not ' +
                                 str(type(filters[f])) + ')')

        newLength = self.data[dataset].shape[0]
        pcntChange = (originalLength - newLength) / originalLength
        pcntChange = round(pcntChange * 100, 1)
        report = 'Filtered dataset from ' + str(originalLength) + ' to ' + \
                 str(newLength) + ' rows (' + str(pcntChange) + ')'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(
            report,
            elapsedSeconds,
            dataset,
            self.data[dataset],
            shapeOnly=True)

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

        self.data[dataset][cleanedColumn] = \
            cleaningFunc(self.data[dataset][column])

        # If the dataset has audit cols, then we just added after them,
        # so sort the audit cols back to the end
        auditColList = self.conf.auditColumns['colNames'].tolist()
        colList = list(self.data[dataset])
        isAuditColsInDataset = \
            set(auditColList).issubset(list(self.data[dataset]))
        if isAuditColsInDataset:
            colList = [col for col in list(self.data[dataset])
                       if col not in auditColList]
            colList = list(self.data[dataset]) + auditColList
        df_reordered = self.data[dataset][colList]

        report = 'Cleaned ' + column + ' to ' + cleanedColumn
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, df_reordered)

    def union(self, datasets, targetDataset, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        try:
            self.data[targetDataset] = \
                pd.concat([self.data[dataset] for dataset in datasets])
        except AssertionError:
            error = ''
            for dataset in datasets:
                error += '** ' + dataset + ' (sorted) **\n'
                error += '\n'
                error += pprint.pformat(sorted(list(
                    self.data[dataset].columns)))
                error += '\n'
                error += '\n'
            logger.logStepError(error)
            raise

        report = 'Concatenated ' + str(len(datasets)) + \
                 ' dfs, totalling ' + \
                 str(self.data[targetDataset].shape[0]) + ' rows'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(
            report,
            elapsedSeconds,
            targetDataset,
            self.data[targetDataset])

    def getColumns(self, dataset, columnNames, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        if isinstance(columnNames, str):
            cols = self.data[dataset][columnNames]
        elif isinstance(columnNames, list):
            cols = {}
            for columnName in columnNames:
                cols[columnName] = self.data[dataset][columnName]
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
            dfs = self.data[datasets].copy()
        elif isinstance(datasets, list):
            dfs = {}
            for dataset in datasets:
                dfs[dataset] = datasets[dataset].copy()
        else:
            raise ValueError('datasets must be string or list')

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

        return dfs

    def duplicateDataset(self, dataset, targetDatasets, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        for targetDataset in targetDatasets:
            self.data[targetDataset] = self.data[dataset].copy()

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

    def dedupe(self, dataset, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        self.data[dataset].drop_duplicates(inplace=True)

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, self.data[dataset])

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

        self.data[targetDataset] = pd.merge(
            self.data[datasets[0]],
            self.data[datasets[1]],
            on=joinCol,
            how=how)
        if keepCols is not None:
            self.data[targetDataset] = self.data[targetDataset][keepCols]

        report = 'Joined datasets ' + datasets[0] + ' ('
        report += str(self.data[datasets[0]].shape[0]) + ' rows) and '
        report += datasets[1] + ' ('
        report += str(self.data[datasets[1]].shape[0]) + ' rows) into '
        report += targetDataset + ' ('
        report += str(self.data[targetDataset].shape[0]) + ' rows)'
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(
            report,
            elapsedSeconds,
            targetDataset,
            self.data[targetDataset])

    def setAuditCols(self, dataset, bulkOrDelta, sourceSystem, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        if bulkOrDelta == 'BULK':
            self.data[dataset]['audit_source_system'] = sourceSystem
            self.data[dataset]['audit_bulk_load_date'] = datetime.now()
            self.data[dataset]['audit_latest_delta_load_date'] = None
            self.data[dataset]['audit_latest_load_operation'] = 'BULK'

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

    def createAuditNKs(self, dataset, desc):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        # TODO will improve this over time. Basic solution as PoC
        self.data[dataset]['nk_audit'] = \
            self.data[dataset]['audit_latest_load_operation'] + '_' + '10'

        self.data[dataset].drop(
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

        colList = list(self.data[dataset])

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds)

        return colList

    def customSQL(self, sql, dataLayer, dataset=None, desc=''):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc, additionalDesc=sql)

        datastore = self.conf.getLogicalDataModel(dataLayer).datastore

        if dataset is not None:
            self.data[dataset] = dbIO.customSQL(sql, datastore)
        else:
            dbIO.customSQL(sql, datastore)

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        if dataset is not None:
            logger.logStepEnd(
                report,
                elapsedSeconds,
                dataset,
                self.data[dataset])
        else:
            logger.logStepEnd(report, elapsedSeconds, dataset)

    def iterate(self, dataset, function, desc=None):
        startTime = datetime.now()
        logger.logStepStart(startTime, desc)

        for row in self.data[dataset].itertuples():
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
        # self.data[dataset]

        report = ''
        elapsedSeconds = (datetime.now() - startTime).total_seconds()
        logger.logStepEnd(report, elapsedSeconds, dataset, self.data[dataset])

    def __str__(self):
        op = ''
        op += 'DataFlow: ' + self.description + '\n'
        op += '  Datasets: \n'
        for dataset in self.data:
            op += '    - ' + dataset + '\n'
        op += '\n'
        return op

# logger.logClearedTempData()
