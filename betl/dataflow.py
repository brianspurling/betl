import pandas as pd
import os
import pprint
from datetime import datetime

from . import logger
from . import fileIO
from . import dbIO
from . import gsheetIO
from . import excelIO
from . import alerts


class DataFlow():

    def __init__(self, conf, desc):

        self.dflStartTime = datetime.now()
        self.currentStepStartTime = None
        self.currentStepId = None
        self.conf = conf
        self.description = desc
        self.data = {}
        # trgDataset is always set to the most recent dataset written to disk
        self.trgDataset = None
        logger.logDFStart(self.description, self.dflStartTime)
        self.dataflowId = self.conf.CTRL.CTRL_DB.insertDataflow(
            dataflow={
                'execId': self.conf.STATE.EXEC_ID,
                'functionId': self.conf.STATE.FUNCTION_ID,
                'description': self.description})

    def close(self):
        elapsedSeconds = (datetime.now() - self.dflStartTime).total_seconds()
        logger.logDFEnd(elapsedSeconds, self.trgDataset)

        if self.trgDataset is not None:
            rowCount = self.trgDataset.shape[0]
            colCount = self.trgDataset.shape[1]
        else:
            rowCount = None
            colCount = None

        self.conf.CTRL.CTRL_DB.updateDataflow(
            dataflowId=self.dataflowId,
            status='SUCCESSFUL',
            rowCount=rowCount,
            colCount=colCount)

        # By removing all keys, we remove all pointers to the dataframes,
        # hence making them available to Python's garbage collection
        self.data.clear()
        del(self.trgDataset)

    def getDataFromSrc(self, tableName, srcSysID, desc, mappedTableName=None):

        self.stepStart(desc=desc)

        srcSysDatastore = self.conf.DATA.getSrcSysDatastore(srcSysID)

        limitdata = self.conf.EXE.DATA_LIMIT_ROWS

        self.data[tableName] = pd.DataFrame()

        if mappedTableName is not None:
            srcTableName = mappedTableName
        else:
            # Cut off the src_<dataModelID>_ prefix, by doing
            # two "left trims" on the "_" char
            srcTableName = tableName[tableName.find("_")+1:]
            srcTableName = srcTableName[srcTableName.find("_")+1:]

        if srcSysDatastore.datastoreType == 'FILESYSTEM':

            path = srcSysDatastore.path
            separator = srcSysDatastore.delim
            quotechar = srcSysDatastore.quotechar

            if srcSysDatastore.fileExt == '.csv':
                fileNameMap = self.conf.STATE.FILE_NAME_MAP
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

    def read(self,
             tableName,
             dataLayer,
             targetDataset=None,
             forceDBRead=False,
             desc=None):

        if desc is None:
            desc = 'Read data from ' + dataLayer + '.' + tableName + \
                   ' (forceDBWrite = ' + str(forceDBRead) + ')'
        self.stepStart(desc=desc)

        _targetDataset = tableName
        if targetDataset is not None:
            _targetDataset = targetDataset

        if _targetDataset in self.data:
            raise ValueError('There is already a dataset named ' +
                             _targetDataset + ' in this dataflow')

        path = (self.conf.CTRL.TMP_DATA_PATH + dataLayer + '/')
        filename = tableName + '.csv'

        self.data[_targetDataset] = pd.DataFrame()

        if forceDBRead:
            dbID = self.conf.DATA.getDataLayerLogicalSchema(dataLayer).databaseID
            self.data[_targetDataset] = dbIO.readDataFromDB(
                tableName=tableName,
                conn=self.conf.DATA.getDWHDatastore(dbID).conn)

        else:
            fileNameMap = self.conf.STATE.FILE_NAME_MAP
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

        self.trgDataset = self.data[dataset]

        # Work out whether we need to write to DB as well as CSV
        writeToDB = False

        if dataLayerID in ['TRG', 'SUM']:
            writeToDB = True
        elif forceDBWrite:
            writeToDB = True
        else:
            writeToDB = self.conf.EXE.WRITE_TO_ETL_DB
        dataLayer = self.conf.DATA.getDataLayerLogicalSchema(dataLayerID)
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
                self.conf.DATA.AUDIT_COLS['colNames'].tolist()
            colsIncludeSKs = False
            colsIncludeAudit = False
            for colName in list(self.trgDataset):
                if colName in logDataModelColNames_sks:
                    colsIncludeSKs = True
                if colName in self.conf.DATA.AUDIT_COLS['colNames'].tolist():
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
                        self.conf.DATA.AUDIT_COLS['colNames'].tolist()
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

        path = (self.conf.CTRL.TMP_DATA_PATH + dataLayerID + '/')
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

        self.stepEnd(report=report, df=self.trgDataset)
        if not keepDataflowOpen:
            self.close()

    def dropColumns(self,
                    dataset,
                    colsToDrop=None,
                    colsToKeep=None,
                    desc=None,
                    dropAuditCols=False):

        if desc is None:
            desc = 'Dropping columns from ' + dataset + ': ' + str(colsToDrop)
        self.stepStart(desc=desc)

        if colsToDrop is not None and colsToKeep is not None:
            raise ValueError("Nope!")

        auditCols = self.conf.DATA.AUDIT_COLS['colNames'].tolist()
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

        self.stepEnd(
            report=report,
            datasetName=dataset,
            df=self.data[dataset])

    def renameColumns(self, dataset, columns, desc):

        self.stepStart(desc=desc)

        self.data[dataset].rename(index=str,
                                  columns=columns,
                                  inplace=True)

        report = 'Renamed ' + str(len(columns)) + ' columns'

        self.stepEnd(
            report=report,
            datasetName=dataset,
            df=self.data[dataset])

    def addColumns(self, dataset, columns, desc):

        self.stepStart(desc=desc)

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

        self.stepEnd(
            report=report,
            datasetName=dataset,
            df=self.data[dataset])

    def setColumns(self, dataset, columns, desc):
        # A wrapper for semantic purposes
        self.addColumns(dataset, columns, desc)

    def setNulls(self, dataset, columns, desc):

        self.stepStart(desc=desc)

        for col in columns:
            self.data[dataset].loc[self.data[dataset][col].isnull(), col] = \
                columns[col]

        report = ''

        self.stepEnd(report=report)

    def filter(self, dataset, filters, desc):

        self.stepStart(desc=desc)

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
                elif filters[f][0] == '!=':
                    self.data[dataset] = \
                        self.data[dataset].loc[
                            self.data[dataset][f] != filters[f][1]]
                else:
                    raise ValueError(
                        'Filter currently only support ==, !=, < or >')
            else:
                raise ValueError('filter value must be str or tuple (not ' +
                                 str(type(filters[f])) + ')')

        newLength = self.data[dataset].shape[0]
        pcntChange = (originalLength - newLength) / originalLength
        pcntChange = round(pcntChange * 100, 1)
        report = 'Filtered dataset from ' + str(originalLength) + ' to ' + \
                 str(newLength) + ' rows (' + str(pcntChange) + ')'

        self.stepEnd(
            report=report,
            datasetName=dataset,
            df=self.data[dataset],
            shapeOnly=True)

    def toNumeric(self,
                  dataset,
                  columns,
                  desc,
                  cleanedColumns=None,
                  cast=None):

        self.stepStart(desc=desc)

        if isinstance(columns, str):
            columns = [columns]

        cnt = 0
        for column in columns:

            if cleanedColumns is None:
                cleanedColumn = column
            else:
                cleanedColumn = cleanedColumns[cnt]
            cnt += 1

            self.data[dataset][cleanedColumn] = \
                self.data[dataset][column].str.replace(r"[^\d.]+", '')

            self.data[dataset][cleanedColumn] = \
                pd.to_numeric(
                    arg=self.data[dataset][cleanedColumn],
                    errors='coerce')

            if cast == 'integer':
                self.data[dataset][cleanedColumn] = \
                    self.data[dataset][cleanedColumn].fillna(0).astype(int)
            else:
                raise ValueError('You tried to cast to a type not yet handled by' +
                                 ' dataflow.toNumeric: ' + cast)

            # TODO need to pick up look here
            report = 'Cleaned ' + column + ' to ' + cleanedColumn

        self.stepEnd(
            report=report,
            datasetName=dataset,
            df=self.data[dataset])

    def cleanColumn(self,
                    dataset,
                    cleaningFunc,
                    columns,
                    desc,
                    cleanedColumns=None):

        self.stepStart(desc=desc)

        cnt = 0
        cleanedColName = ''
        for column in columns:
            if cleanedColumns is None:
                cleanedColName = column
            else:
                cleanedColName = cleanedColumns[cnt]
            cnt += 1

            self.data[dataset][cleanedColName] = \
                cleaningFunc(self.data[dataset][column])

        self.stepEnd(
            report='',
            datasetName=dataset,
            df=self.data[dataset])

    def union(self, datasets, targetDataset, desc):

        self.stepStart(desc=desc)

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

        self.stepEnd(
            report=report,
            datasetName=targetDataset,
            df=self.data[targetDataset])

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

    def duplicateDataset(self, dataset, targetDatasets, desc):

        self.stepStart(desc=desc)

        for targetDataset in targetDatasets:
            self.data[targetDataset] = self.data[dataset].copy()

        report = ''

        self.stepEnd(report=report)

    def dedupe(self, dataset, desc):

        self.stepStart(desc=desc)

        self.data[dataset].drop_duplicates(inplace=True)

        report = ''

        self.stepEnd(
            report=report,
            datasetName=dataset,
            df=self.data[dataset])

    def join(self,
             datasets,
             targetDataset,
             how,
             desc,
             joinCol=None,
             leftJoinCol=None,
             rightJoinCol=None,
             keepCols=None):

        self.stepStart(desc=desc)

        if len(datasets) > 2:
            raise ValueError('You can only join two tables at once')

        self.data[targetDataset] = pd.merge(
            self.data[datasets[0]],
            self.data[datasets[1]],
            on=joinCol,
            left_on=leftJoinCol,
            right_on=rightJoinCol,
            how=how)
        if keepCols is not None:
            self.data[targetDataset] = self.data[targetDataset][keepCols]

        report = 'Joined datasets ' + datasets[0] + ' ('
        report += str(self.data[datasets[0]].shape[0]) + ' rows) and '
        report += datasets[1] + ' ('
        report += str(self.data[datasets[1]].shape[0]) + ' rows) into '
        report += targetDataset + ' ('
        report += str(self.data[targetDataset].shape[0]) + ' rows)'

        self.stepEnd(
            report=report,
            datasetName=targetDataset,
            df=self.data[targetDataset])

    def setAuditCols(self, dataset, bulkOrDelta, sourceSystem, desc):

        self.stepStart(desc=desc)

        if bulkOrDelta == 'BULK':
            self.data[dataset]['audit_source_system'] = sourceSystem
            self.data[dataset]['audit_bulk_load_date'] = datetime.now()
            self.data[dataset]['audit_latest_delta_load_date'] = None
            self.data[dataset]['audit_latest_load_operation'] = 'BULK'

        report = ''

        self.stepEnd(report=report)

    def createAuditNKs(self, dataset, desc):

        self.stepStart(desc=desc)

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

        self.stepEnd(report=report)

    def getColumnList(self, dataset, desc=None):

        if desc is None:
            desc = 'Get column list for ' + dataset

        self.stepStart(desc=desc)

        colList = list(self.data[dataset])

        report = ''

        self.stepEnd(report=report)

        return colList

    def customSQL(self, sql, desc, dataLayer, dataset=None):

        self.stepStart(desc=desc, additionalDesc=sql)

        datastore = self.conf.DATA.getDataLayerLogicalSchema(dataLayer).datastore

        if dataset is not None:
            self.data[dataset] = dbIO.customSQL(sql, datastore)
        else:
            dbIO.customSQL(sql, datastore)

        report = ''

        if dataset is not None:
            self.stepEnd(
                report=report,
                datasetName=dataset,
                df=self.data[dataset])
        else:
            self.stepEnd(
                report=report,
                datasetName=dataset)

    def iterate(self, dataset, function, desc):

        self.stepStart(desc=desc)

        for row in self.data[dataset].itertuples():
            function(row)

        report = ''

        self.stepEnd(report=report)

    def truncate(self, dataset, dataLayerID, desc, forceDBWrite=False):
        self.stepStart(desc=desc)

        path = (self.conf.CTRL.TMP_DATA_PATH + dataLayerID + '/')
        filename = dataset + '.csv'

        fileIO.truncateFile(self.conf, path, filename)

        if forceDBWrite:
            dataLayer = self.conf.DATA.getDataLayerLogicalSchema(dataLayerID)
            dbIO.truncateTable(dataset, dataLayer.datastore)

        report = ''

        self.stepEnd(report=report)

    # TODO: I can't stop eg. '16 ' being converted to '16' when it gets
    # written to Sheets, which can created dupes in the mapping cols.
    # So trim in app before calling this :(
    def mapMasterData(self,
                      dataset,
                      mdmWS,
                      joinCols,
                      masterDataCols,
                      desc,
                      autoPopMappingCols=[]):

        self.stepStart(desc=desc)

        ############################
        # Connect to MDM Worksheet #
        ############################

        ws = self.conf.DATA.getMDMDatastore().conn.worksheet(mdmWS)

        #################################
        # Extract the current MDM table #
        #################################

        mdm_list = ws.get_all_values()

        if(len(mdm_list)) == 0:
            raise ValueError('MDM column headings must be entered into the ' +
                             'Google Sheet')
        if(len(mdm_list)) == 1:
            df_mdm = pd.DataFrame(columns=mdm_list[0:1][0])
        else:
            df_mdm = pd.DataFrame(mdm_list[1:], columns=mdm_list[0:1][0])

        #####################
        # Fill NaNs in data #
        #####################

        # NaNs won't join to empty cells in our Gsheet, so we replace
        self.data[dataset] = self.data[dataset].fillna(value='')

        ##########################
        # Rename autoPop columns #
        ##########################

        # We might have masterDataCols in our dataset, so we will rename
        # them before we join to allow us to control them later
        # Note that we assume any autoPop map columns have been joined to
        # the main dataset with the same logic the MDM will be using (i.e.
        # the join columns). Which would mean that you couldn't have
        # two identical joinCol combos with different autoPopMapCols.
        # This is important, because we will group below before writing back
        # to the MDM spreadsheet, and we'll get multiple rows
        # where we don't want them if this is not the case.
        renamedAutoPopColNames = []
        for colName in autoPopMappingCols:
            autoPopColName = 'autoPop_' + colName
            self.data[dataset].rename(index=str,
                                      columns={colName: autoPopColName},
                                      inplace=True)
            renamedAutoPopColNames.append(autoPopColName)

        ##########################
        # Merge the two datasets #
        ##########################

        df_m = pd.merge(
            self.data[dataset],
            df_mdm,
            on=joinCols,
            how='outer',
            indicator=True)

        #################
        # Auto Populate #
        #################

        # AutoPop does not overwrite data already in the MDM spreadsheet

        for autoPopCol in renamedAutoPopColNames:
            mapCol = autoPopCol[8:]
            df_m.loc[df_m[mapCol].isnull(), mapCol] = \
                df_m[df_m[mapCol].isnull()][autoPopCol]
            df_m.loc[df_m[mapCol] == '', mapCol] = \
                df_m[df_m[mapCol] == ''][autoPopCol]

        numOfRowsWithMatchingMDM = \
            len(df_m.loc[df_m['_merge'] == 'both'].index)
        numOfRowsWithoutMatchingMDM = \
            len(df_m.loc[df_m['_merge'] == 'left_only'].index)
        numOfUnmatchedRowsInMDM = \
            len(df_m.loc[df_m['_merge'] == 'right_only'].index)

        #########################
        # Drop unneeded columns #
        #########################

        if 'count' in list(df_m):
            colsToDrop = ['count'] + renamedAutoPopColNames
        else:
            colsToDrop = renamedAutoPopColNames
        df_m.drop(
            labels=colsToDrop,
            axis=1,
            inplace=True)

        ###################################
        # Replace NaNs with empty strings #
        ###################################

        # If the mapping was missing, we treat it as though it was a blank
        # cell in the Gsheet
        df_m[masterDataCols] = df_m[masterDataCols].fillna(value='')

        ###############################
        # Update our output dataframe #
        ###############################

        self.data[dataset] = df_m.loc[df_m['_merge'] != 'right_only'].copy()
        self.data[dataset].drop(
            labels='_merge',
            axis=1,
            inplace=True)

        ######################################################
        # Prepare new MDM dataset for write back (to gsheet) #
        ######################################################

        # Drop any additional columns (not part of the MDM dataset)
        colsToDrop = list(df_m)
        colsToDrop = \
            [x for x in list(df_m) if x not in joinCols + masterDataCols]
        df_m.drop(
            labels=colsToDrop,
            axis=1,
            inplace=True)

        # Group up to distinct set of rows
        df_m = df_m.groupby(
            df_m.columns.tolist(),
            sort=False,
            as_index=False).size().reset_index(name='count')
        df_m.sort_values(by='count', ascending=False, inplace=True)

        ###############################
        # Count up empty mapping rows #
        ###############################

        numOfMDMRowsWithNoFilledInValues = 0
        for i, row in df_m.iterrows():
            rowIsAllBlank = True
            for colName in masterDataCols:
                if row[colName] != '':
                    rowIsAllBlank = False
            if rowIsAllBlank:
                numOfMDMRowsWithNoFilledInValues += 1

        ####################
        # Write to GSheets #
        ####################

        colsToWriteBack = joinCols + masterDataCols + ['count']
        ws.resize(rows=len(df_m)+1, cols=len(colsToWriteBack))
        cell_list = ws.range(1, 1, len(df_m)+1, len(colsToWriteBack))
        cellPos = 0
        for colName in colsToWriteBack:
            cell_list[cellPos].value = colName
            cellPos += 1
        for i, row in df_m.iterrows():
            for colName in colsToWriteBack:
                value = str(row[colName])
                if value == '£$%^NOTAVALUE^%$£':
                    value = ''
                cell_list[cellPos].value = value
                cellPos += 1
        ws.clear()
        ws.update_cells(cell_list)

        ###########
        # Wrap Up #
        ###########

        r = ''

        r += ('For MDM ' + mdmWS + ' there were \n'
              '       - ' + str(numOfRowsWithoutMatchingMDM) + ' rows in ' +
              'the data without a corresponding MDM row (these have now ' +
              'been added)\n' +
              '       - There are now ' +
              str(numOfMDMRowsWithNoFilledInValues) + ' rows in the MDM ' +
              'without any mapped values filled in\n')
        if (numOfRowsWithoutMatchingMDM > 0 or
                numOfMDMRowsWithNoFilledInValues > 0):
            alerts.logAlert(self.conf, r)
        r += ('       - ' + str(numOfRowsWithMatchingMDM) + ' rows in the ' +
              'data were matched to corresponding MDM rows, and\n' +
              '       - ' + str(numOfUnmatchedRowsInMDM) + ' rows in the ' +
              'MDM did not find a match in the data')

        if len(self.data[dataset]) == 0:
            self.stepEnd(
                report=r,
                datasetName=dataset,
                shapeOnly=False)
        else:
            self.stepEnd(
                report=r,
                datasetName=dataset,
                df=self.data[dataset],
                shapeOnly=False)

    def pivotColsToRows(self,
                        dataset,
                        colsNotToPivot,
                        colsToPivot,
                        varName,
                        valueName,
                        desc):

        self.stepStart(desc=desc)

        auditCols = self.conf.DATA.AUDIT_COLS['colNames'].tolist()

        self.data[dataset] = pd.melt(
            self.data[dataset],
            id_vars=colsNotToPivot + auditCols,
            value_vars=colsToPivot,
            var_name=varName,
            value_name=valueName)

        report = ''

        self.stepEnd(
            report=report,
            datasetName=dataset,  # optional
            df=self.data[dataset],  # optional
            shapeOnly=False)  # optional


    def templateStep(self, dataset, desc):

        self.stepStart(desc=desc)

        # self.data[dataset]

        report = ''

        self.stepEnd(
            report=report,
            datasetName=dataset,  # optional
            df=self.data[dataset],  # optional
            shapeOnly=False)  # optional

    def __str__(self):
        op = ''
        op += 'DataFlow: ' + self.description + '\n'
        op += '  Datasets: \n'
        for dataset in self.data:
            op += '    - ' + dataset + '\n'
        op += '\n'
        return op

    def stepStart(self,
                  desc,
                  datasetName=None,
                  df=None,
                  additionalDesc=None):

        self.currentStepStartTime = datetime.now()

        logger.logStepStart(
            startTime=self.currentStepStartTime,
            desc=desc,
            datasetName=datasetName,
            df=df,
            additionalDesc=additionalDesc)

        self.currentStepId = self.conf.CTRL.CTRL_DB.insertStep(
            step={
                'execId': self.conf.STATE.EXEC_ID,
                'dataflowID': self.dataflowId,
                'description': desc})

    def stepEnd(self,
                report,
                datasetName=None,
                df=None,
                shapeOnly=False):

        elapsedSeconds = \
            (datetime.now() - self.currentStepStartTime).total_seconds()

        logger.logStepEnd(
            report=report,
            duration=elapsedSeconds,
            datasetName=datasetName,
            df=df,
            shapeOnly=shapeOnly)

        if df is not None:
            rowCount = df.shape[0]
            colCount = df.shape[1]
        else:
            rowCount = None
            colCount = None

        self.conf.CTRL.CTRL_DB.updateStep(
            stepId=self.currentStepId,
            status='SUCCESSFUL',
            rowCount=rowCount,
            colCount=colCount)
