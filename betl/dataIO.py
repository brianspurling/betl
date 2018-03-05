import inspect
import datetime
import pandas as pd
import os
from .dbIO import DatabaseIO
from .gsheetIO import GsheetIO
from .fileIO import FileIO
from . import logger

TMP_FILE_SUBDIR_MAPPING = {}


class DataIO():

    def __init__(self, conf):

        self.devLog = logger.getDevLog(__name__)
        self.jobLog = logger.getJobLog()

        self.conf = conf

        self.fileIO = FileIO(conf)
        self.dbIO = DatabaseIO(conf)
        self.gsheetIO = GsheetIO(conf)

        self.rebuildTmeFileSubdirMapping()

    def readDataFromCsv(self,
                        file_or_filename,
                        pathOverride=None,
                        sep=',',
                        quotechar='"',
                        callingFuncName=None):

        if callingFuncName is None:
            callingFuncName = inspect.stack()[2][3]

        df = pd.DataFrame()
        filename = ''
        if type(file_or_filename) == str:
            filename = file_or_filename
        else:
            filename = file_or_filename.name

        path = ''
        if pathOverride is not None:
            path = pathOverride + filename
        else:
            tmpDataPath = self.conf.app.TMP_DATA_PATH
            stage = TMP_FILE_SUBDIR_MAPPING[filename]

            path = tmpDataPath + stage + '/' + filename + '.csv'

        self.jobLog.info(logger.logStepStart(
                         'Reading data from CSV: ' + path,
                         callingFuncName=callingFuncName))

        df = self.fileIO.readDataFromCsv(path=path,
                                         sep=sep,
                                         quotechar=quotechar)

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

    def readDataFromDB(self,
                       tableName,
                       datastore,
                       cols='*',
                       callingFuncName=None):

        if callingFuncName is None:
            callingFuncName = inspect.stack()[2][3]

        self.jobLog.info(logger.logStepStart(
            'Reading data ' + tableName + ' from ' + datastore.dbID + ' DB',
            callingFuncName=callingFuncName))

        df = self.dbIO.readDataFromDB(tableName=tableName,
                                      conn=datastore.conn,
                                      cols=cols)

        self.jobLog.info(logger.logStepEnd(df))

        return df

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
                       file_or_filename,
                       mode,
                       callingFuncName=None):

        global TMP_FILE_SUBDIR_MAPPING

        if callingFuncName is None:
            callingFuncName = inspect.stack()[2][3]

        filename = ''
        headers = True
        if type(file_or_filename) == str:
            filename = file_or_filename
        else:
            headers = False
            filename = file_or_filename.name

        tmpDataPath = self.conf.app.TMP_DATA_PATH
        stage = self.conf.state.STAGE
        TMP_FILE_SUBDIR_MAPPING[filename] = stage
        path = (tmpDataPath + stage + '/' +
                filename + '.csv')
        if not os.path.exists(tmpDataPath + stage + '/'):
            os.makedirs(tmpDataPath + stage + '/')

        self.jobLog.info(logger.logStepStart(
            'Writing data to CSV: ' + filename,
            callingFuncName=callingFuncName))

        self.fileIO.writeDataToCsv(df, path, headers, mode)

        self.jobLog.info(logger.logStepEnd(df))

    def writeDataToDB(self,
                      df,
                      tableName,
                      dbEng,
                      if_exists,
                      callingFuncName=None):

        if callingFuncName is None:
            callingFuncName = inspect.stack()[2][3]

        self.jobLog.info(logger.logStepStart(
            'Writing data to DB: ' + tableName,
            callingFuncName=callingFuncName))

        self.dbIO.writeDataToDB(df, tableName, dbEng, if_exists)

        self.jobLog.info(logger.logStepEnd(df))

    # # # #

    def readDataFromSrcSys(self, srcSysID, file_name_or_table_name):

        callingFuncName = inspect.stack()[1][3]
        srcSysDatastore = self.conf.app.SRC_SYSTEMS[srcSysID]

        df = pd.DataFrame()

        if srcSysDatastore.datastoreType == 'FILESYSTEM':
            filename = file_name_or_table_name
            path = srcSysDatastore.path
            separator = srcSysDatastore.delim
            quotechar = srcSysDatastore.quotechar

            if srcSysDatastore.fileExt == '.csv':
                df = self.readDataFromCsv(file_or_filename=filename + '.csv',
                                          pathOverride=path,
                                          sep=separator,
                                          quotechar=quotechar,
                                          callingFuncName=callingFuncName)
            else:
                raise ValueError('Unhandled file extension for src system: ' +
                                 srcSysDatastore.fileExt + ' for source sys ' +
                                 srcSysID)

        elif srcSysDatastore.datastoreType == 'POSTGRES':

            etlTableName = file_name_or_table_name
            # Cut off the src_<dataModelID>_ prefix, by doing
            # two "left trims" on the "_" char
            srcTableName = etlTableName[etlTableName.find("_")+1:]
            srcTableName = srcTableName[srcTableName.find("_")+1:]

            df = self.readDataFromDB(tableName=srcTableName,
                                     datastore=srcSysDatastore,
                                     cols='*',
                                     callingFuncName=callingFuncName)

        elif srcSysDatastore.datastoreType == 'SPREADSHEET':
            df = self.readDataFromSpreadsheet(
                tableName=file_name_or_table_name,
                datastore=srcSysDatastore,
                callingFuncName=callingFuncName)
        else:
            raise ValueError('Extract for source systems type <'
                             + srcSysDatastore.datastoreType
                             + '> connection type not supported')

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

    def readDataFromEtlDB(self, tableName):

        callingFuncName = inspect.stack()[2][3]

        df = self.readDataFromDB(
            tableName=tableName,
            datastore=self.conf.app.DWH_DATABASES['ETL'],
            cols='*',
            callingFuncName=callingFuncName)

        return df

    def readFromTrgDB(self, tableName, columnList):

        callingFuncName = inspect.stack()[2][3]

        newColList = []
        for col in columnList:
            newColList.append('"' + col + '"')
        cols = ",".join(newColList)
        df = self.readDataFromDB(tableName=tableName,
                                 datastore=self.conf.app.DWH_DATABASES['TRG'],
                                 cols=cols,
                                 callingFuncName=callingFuncName)

        return df

    def writeDataToTrgDB(self, df, tableName, if_exists):

        callingFuncName = inspect.stack()[2][3]

        dbEng = self.conf.app.DWH_DATABASES['TRG'].eng
        self.writeDataToDB(df, tableName, dbEng, if_exists, callingFuncName)

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
        colList = [skColName] + nkColList
        df_sk = self.readFromTrgDB(tableName, colList)

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

        self.writeDataToCsv(df_sk, 'sk_' + tableName, 'w')

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
        print(df.shape)
        df_merged = pd.merge(df, df_sk, on=nkColName, how='left')
        self.jobLog.info(logger.logStepEnd(df_merged))

        self.jobLog.info(logger.logStepStart(
            'dropping unneeded column: ' + nkColName))
        df_merged.drop(nkColName, axis=1, inplace=True)
        self.jobLog.info(logger.logStepEnd(df_merged))
        del df_sk
        return df_merged

    def rebuildTmeFileSubdirMapping(self):
        global TMP_FILE_SUBDIR_MAPPING
        for path, subdirs, files in os.walk(self.conf.app.TMP_DATA_PATH):
            for name in files:
                TMP_FILE_SUBDIR_MAPPING[name.replace('.csv', '')] = \
                    path.replace(self.conf.app.TMP_DATA_PATH, '')
