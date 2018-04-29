import inspect
import pandas as pd

from .dbIO import DatabaseIO
from .gsheetIO import GsheetIO
from .fileIO import FileIO
from . import logger


class DataIO():

    def __init__(self, conf):

        self.devLog = logger.getDevLog(__name__)
        self.jobLog = logger.getLogger()

        self.conf = conf

        self.fileIO = FileIO(conf)
        self.dbIO = DatabaseIO(conf)
        self.gsheetIO = GsheetIO(conf)

    def readData(self, tableName, dataLayerID, forceDBRead=None):

        callingFuncName = inspect.stack()[2][3]

        path = (self.conf.app.TMP_DATA_PATH +
                dataLayerID + '/')
        filename = tableName + '.csv'

        self.jobLog.info(logger.logStepStart(
                         'Reading data from CSV: ' + path + filename,
                         callingFuncName=callingFuncName))

        df = pd.DataFrame()
        if forceDBRead is not None:
            df = self.dbIO.readDataFromDB(
                tableName=tableName,
                conn=self.conf.app.DWH_DATABASES[forceDBRead].conn)

        else:
            df = self.fileIO.readDataFromCsv(path=path,
                                             filename=filename,
                                             sep=',',
                                             quotechar='"')

        # We never want audit cols to come into transform dataframes
        # if 'audit_source_system' in df.columns:
        #     df.drop(['audit_source_system'], axis=1, inplace=True)
        # if 'audit_bulk_load_date' in df.columns:
        #     df.drop(['audit_bulk_load_date'], axis=1, inplace=True)
        # if 'audit_latest_delta_load_date' in df.columns:
        #     df.drop(['audit_latest_delta_load_date'], axis=1, inplace=True)
        # if 'audit_latest_delta_load_operation' in df.columns:
        #     df.drop(['audit_latest_delta_load_operation'],
        #             axis=1,
        #             inplace=True)

        self.jobLog.info(logger.logStepEnd(df))

        return df

    def writeData(self, df, tableName, dataLayerID, append_or_replace,
                  forceDBWrite=False,
                  writingDefaultRows=False):

        callingFuncName = inspect.stack()[2][3]

        # Work out whether we need to write to DB as well as CSV
        writeToDB = False

        if dataLayerID in ['TRG', 'SUM']:
            writeToDB = True
        elif forceDBWrite:
            writeToDB = True
        else:
            writeToDB = self.conf.exe.WRITE_TO_ETL_DB

        dataLayer = self.conf.state.LOGICAL_DATA_MODELS[dataLayerID]
        if tableName not in dataLayer.getListOfTables() and not forceDBWrite:
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
        logDataModelCols = dataLayer.getColumnsForTable(tableName)
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
                self.conf.auditColumns['colName'].tolist()
            colsIncludeSKs = False
            colsIncludeAudit = False
            for colName in list(df):
                if colName in logDataModelColNames_sks:
                    colsIncludeSKs = True
                if colName in self.conf.auditColumns['colName'].tolist():
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
                        self.conf.auditColumns['colName'].tolist()
                if not colsIncludeSKs and not colsIncludeAudit:
                    colsToSortBy = logDataModelColNames_noSKs
                df = df[colsToSortBy]

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
            self.writeDataToDB(
                df,
                tableName,
                dbEng,
                append_or_replace,
                callingFuncName)

        # write to CSV
        mode = 'w'
        if append_or_replace.upper() == 'APPEND':
            mode = 'a'

        if writingDefaultRows:
            df.drop(df.columns[0], axis=1, inplace=True)

        self.writeDataToCsv(
            df,
            tableName,
            dataLayerID,
            mode,
            callingFuncName)

    def getColumnHeadings(self, tableName, dataLayerID):

        callingFuncName = inspect.stack()[2][3]

        path = (self.conf.app.TMP_DATA_PATH +
                dataLayerID + '/')
        filename = tableName + '.csv'

        self.jobLog.info(logger.logStepStart(
                         'Getting column headings from CSV: ' + path
                         + filename,
                         callingFuncName=callingFuncName))

        df = pd.DataFrame()
        df = self.fileIO.readDataFromCsv(path=path,
                                         filename=filename,
                                         sep=',',
                                         quotechar='"',
                                         getFirstRow=True)

        # We never want audit cols to come into transform dataframes
        if 'audit_source_system' in df.columns:
            df.drop(['audit_source_system'], axis=1, inplace=True)
        if 'audit_bulk_load_date' in df.columns:
            df.drop(['audit_bulk_load_date'], axis=1, inplace=True)
        if 'audit_latest_delta_load_date' in df.columns:
            df.drop(['audit_latest_delta_load_date'], axis=1, inplace=True)
        if 'audit_latest_delta_load_operation' in df.columns:
            df.drop(['audit_latest_delta_load_operation'],
                    axis=1,
                    inplace=True)

        self.jobLog.info(logger.logStepEnd(df))

        return list(df)

    def truncateFile(self,
                     filename,
                     dataLayerID,
                     callingFuncName=None):
        if callingFuncName is None:
            callingFuncName = inspect.stack()[2][3]

        path = (self.conf.app.TMP_DATA_PATH +
                dataLayerID + '/')
        _filename = filename + '.csv'
        self.fileIO.truncateFile(path, _filename)

    def writeDataToDB(self,
                      df,
                      tableName,
                      dbEng,
                      if_exists,
                      callingFuncName):

        self.jobLog.info(logger.logStepStart(
            'Writing data to DB: ' + tableName,
            callingFuncName=callingFuncName))

        self.dbIO.writeDataToDB(df, tableName, dbEng, if_exists)

        self.jobLog.info(logger.logStepEnd(df))

    def customSql(self, sql, dataLayerID):

        callingFuncName = inspect.stack()[2][3]

        self.jobLog.info(logger.logStepStart(
            'Executing custom sql\n\n' + sql,
            callingFuncName=callingFuncName))
        datastore = self.conf.state.LOGICAL_DATA_MODELS[dataLayerID].datastore
        df = self.dbIO.customSql(sql, datastore)

        self.jobLog.info(logger.logStepEnd(df))

        return df

    def getSKMapping(self, tableName, nkColList, skColName):

        callingFuncName = inspect.stack()[2][3]

        self.jobLog.info(logger.logStepStart(
            'Reading sk mapping from ' + tableName + ' in TRG dataLayer',
            callingFuncName=callingFuncName))

        colList = [skColName] + nkColList
        newColList = []
        for col in colList:
            newColList.append('"' + col + '"')
        cols = ",".join(newColList)

        df_sk = self.dbIO.readDataFromDB(
            tableName=tableName,
            conn=self.conf.app.DWH_DATABASES['TRG'].conn,
            cols=cols)

        self.jobLog.info(logger.logStepEnd(df_sk))

        # Rename the dims ID column to SK
        self.jobLog.info(logger.logStepStart(
            tableName + ': ' +
            "Rename SK col, concat NKs into single col, drop old NK cols", 1))

        df_sk.rename(index=str,
                     columns={skColName: 'sk'},
                     inplace=True)

        df_sk['nk'] = ''
        underscore = ''
        for col in df_sk.columns.values:
            if col == 'sk':
                continue
            if col == 'nk':
                continue
            else:
                self.jobLog.info("concat " + col + ' to the nk col')
                df_sk['nk'] = df_sk['nk'] + underscore + df_sk[col].map(str)
                underscore = '_'
                df_sk.drop(col, axis=1, inplace=True)

        self.jobLog.info(logger.logStepEnd(df_sk))

        self.writeDataToCsv(df_sk, 'sk_' + tableName, 'STG', 'w')

    def mergeFactWithSks(self, df, col):
        nkColName = col.columnName.replace('fk_', 'nk_')
        df_sk = col.getSKlookup()
        self.jobLog.info(logger.logStepStart('Renaming SK lookup columns ' +
                                             'for ' + col.columnName))
        df_sk.rename(index=str,
                     columns={
                        'sk': col.columnName,
                        'nk': nkColName},
                     inplace=True)
        self.jobLog.info(logger.logStepEnd(df_sk))

        self.jobLog.info(logger.logStepStart(
            'Merging SK with fact for column ' + col.columnName))

        df_merged = pd.merge(df, df_sk, on=nkColName, how='left')
        self.jobLog.info(logger.logStepEnd(df_merged))

        self.jobLog.info(logger.logStepStart(
            'Assigning all missing rows to default -1 row'))

        df_merged.loc[df_merged[col.columnName].isnull(), col.columnName] = -1

        self.jobLog.info(logger.logStepEnd(df_merged))

        self.jobLog.info(logger.logStepStart(
            'dropping unneeded column: ' + nkColName))
        df_merged.drop(nkColName, axis=1, inplace=True)
        self.jobLog.info(logger.logStepEnd(df_merged))
        del df_sk
        return df_merged
