import inspect
import datetime
import pandas as pd
import os

from .dbIO import DatabaseIO
from .gsheetIO import GsheetIO
from .fileIO import FileIO
from . import logger


class DataIO():

    def __init__(self, conf):

        self.devLog = logger.getDevLog(__name__)
        self.jobLog = logger.getJobLog()

        self.conf = conf

        self.fileIO = FileIO(conf)
        self.dbIO = DatabaseIO(conf)
        self.gsheetIO = GsheetIO(conf)

    def readData(self, tableName, dataLayerID):

        callingFuncName = inspect.stack()[2][3]

        path = (self.conf.app.TMP_DATA_PATH +
                dataLayerID + '/')
        filename = tableName + '.csv'

        self.jobLog.info(logger.logStepStart(
                         'Reading data from CSV: ' + path,
                         callingFuncName=callingFuncName))

        df = pd.DataFrame()
        df = self.fileIO.readDataFromCsv(path=path,
                                         filename=filename,
                                         sep=',',
                                         quotechar='"')

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

        return df

    def writeData(self, df, tableName, dataLayerID, append_or_replace,
                  forceDBWrite=False):

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
            # TODO: proper error handling here
            raise ValueError("You just attempted to write to a source system!")

        # write to CSV
        mode = 'w'
        if append_or_replace.upper() == 'APPEND':
            mode = 'a'
        self.writeDataToCsv(
            df,
            tableName,
            dataLayerID,
            mode,
            callingFuncName)

        # write to DB
        if writeToDB:
            dbEng = dataLayer.datastore.eng
            self.writeDataToDB(
                df,
                tableName,
                dbEng,
                append_or_replace,
                callingFuncName)

    def getColumnHeadings(self, tableName, dataLayerID):

        callingFuncName = inspect.stack()[2][3]

        path = (self.conf.app.TMP_DATA_PATH +
                dataLayerID + '/')
        filename = tableName + '.csv'

        self.jobLog.info(logger.logStepStart(
                         'Getting column headings from CSV: ' + path,
                         callingFuncName=callingFuncName))

        df = pd.DataFrame()
        df = self.fileIO.readDataFromCsv(path=path,
                                         filename=filename,
                                         sep=',',
                                         quotechar='"',
                                         nrows=1)

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

    def readDataFromSpreadsheet(self,
                                tableName,
                                datastore,
                                callingFuncName=None):

        if callingFuncName is None:
            callingFuncName = inspect.stack()[2][3]

        self.jobLog.info(logger.logStepStart(
            'Reading data from spreadsheet ' + datastore.ssID + ', ' +
            'worksheet: ' + tableName,
            callingFuncName=callingFuncName))

        df = self.gsheetIO.readDataFromWorksheet(
            worksheet=datastore.worksheets[tableName])

        self.jobLog.info(logger.logStepEnd(df))

        return df

    def writeDataToCsv(self,
                       df,
                       filename,
                       dataLayerID,
                       mode,
                       callingFuncName=None):

        if callingFuncName is None:
            callingFuncName = inspect.stack()[2][3]

        headers = True

        path = (self.conf.app.TMP_DATA_PATH +
                dataLayerID + '/')

        if not os.path.exists(path):
            os.makedirs(path)

        _filename = filename + '.csv'

        self.jobLog.info(logger.logStepStart(
            'Writing data to CSV: ' + filename,
            callingFuncName=callingFuncName))

        self.fileIO.writeDataToCsv(df, path, _filename, headers, mode)

        self.jobLog.info(logger.logStepEnd(df))

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

    def customSql(self, sql, dataLayerID, retrieveTableName=None):

        callingFuncName = inspect.stack()[2][3]

        self.jobLog.info(logger.logStepStart(
            'Executing custom sql\n\n' + sql + '\n\n',
            callingFuncName=callingFuncName))
        datastore = self.conf.state.LOGICAL_DATA_MODELS[dataLayerID].datastore
        self.dbIO.customSql(sql, datastore)
        self.jobLog.info(logger.logStepEnd())

        df = None
        if retrieveTableName is not None:
            ds = self.conf.state.LOGICAL_DATA_MODELS[dataLayerID].datastore
            self.jobLog.info(logger.logStepStart(
                'Reading data from DB: ' + retrieveTableName,
                callingFuncName=callingFuncName))
            df = self.dbIO.readDataFromDB(tableName=retrieveTableName,
                                          conn=ds.conn,
                                          cols='*')
            self.jobLog.info(logger.logStepEnd(df))
            self.writeDataToCsv(df, retrieveTableName, 'STG', 'w')

        return df

    # # # #

    def readDataFromSrcSys(self, srcSysID, file_name_or_table_name):

        callingFuncName = inspect.stack()[1][3]
        srcSysDatastore = self.conf.app.SRC_SYSTEMS[srcSysID]

        df = pd.DataFrame()

        self.jobLog.info(logger.logStepStart(
            'Reading data from source system: ' +
            srcSysDatastore.datatoreID + '.' + file_name_or_table_name,
            callingFuncName=callingFuncName))

        if srcSysDatastore.datastoreType == 'FILESYSTEM':
            filename = file_name_or_table_name
            path = srcSysDatastore.path
            separator = srcSysDatastore.delim
            quotechar = srcSysDatastore.quotechar

            if srcSysDatastore.fileExt == '.csv':
                df = self.fileIO.readDataFromCsv(path=path,
                                                 filename=filename + '.csv',
                                                 sep=separator,
                                                 quotechar=quotechar,
                                                 isTmpData=False)

            else:
                raise ValueError('Unhandled file extension for src system: ' +
                                 srcSysDatastore.fileExt + ' for source sys ' +
                                 srcSysID)

        elif srcSysDatastore.datastoreType in ('POSTGRES', 'SQLITE'):

            etlTableName = file_name_or_table_name
            # Cut off the src_<dataModelID>_ prefix, by doing
            # two "left trims" on the "_" char
            srcTableName = etlTableName[etlTableName.find("_")+1:]
            srcTableName = srcTableName[srcTableName.find("_")+1:]

            df = self.dbIO.readDataFromDB(tableName=srcTableName,
                                          conn=srcSysDatastore.conn,
                                          cols='*')

        elif srcSysDatastore.datastoreType == 'SPREADSHEET':
            df = self.readDataFromSpreadsheet(
                tableName=file_name_or_table_name,
                datastore=srcSysDatastore,
                callingFuncName=callingFuncName)
        else:
            raise ValueError('Extract for source systems type <'
                             + srcSysDatastore.datastoreType
                             + '> connection type not supported')

        self.jobLog.info(logger.logStepEnd(df))

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

        return df

    #
    # Functions to set the audit columns on the dataframes, prior to loading
    # into persistent storage
    #
    def setAuditCols(self, df, srcSysID, action):
        # TODO: Do I need to return this, or is df the same object?
        if action == 'BULK':
            return self.setAuditCols_bulk(df, srcSysID)
        elif action == 'INSERT':
            return self.setAuditCols_insert(df, srcSysID)
        elif action == 'UPDATE':
            return self.setAuditCols_update(df, srcSysID)
        elif action == 'DELETE':
            return self.setAuditCols_delete(df)
        else:
            raise ValueError("Incorrect parameter action passed to " +
                             "setAuditCols")

    def setAuditCols_bulk(self, df, srcSysID):
        df['audit_source_system'] = srcSysID
        df['audit_bulk_load_date'] = datetime.datetime.now()
        df['audit_latest_delta_load_date'] = None
        df['audit_latest_delta_load_operation'] = None

        self.devLog.info("END")

        return df

    def setAuditCols_insert(self, df, sourceSystemId):

        df['audit_source_system'] = sourceSystemId
        df['audit_bulk_load_date'] = None
        df['audit_latest_delta_load_date'] = datetime.datetime.now()
        df['audit_latest_delta_load_operation'] = 'INSERT'

        self.devLog.info("END")

        return df

    def setAuditCols_update(self, df, sourceSystemId):

        df['audit_source_system'] = sourceSystemId
        df['audit_latest_delta_load_date'] = datetime.datetime.now()
        df['audit_latest_delta_load_operation'] = 'UPDATE'

        self.devLog.info("END")

        return df

    def setAuditCols_delete(self, df):

        df['audit_latest_delta_load_date'] = datetime.datetime.now()
        df['audit_latest_delta_load_operation'] = 'DELETE'

        self.devLog.info("END")

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
