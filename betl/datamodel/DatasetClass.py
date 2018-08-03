from .TableClass import Table
from .TableClass import TrgTable

#
# A data model is a collection of tables, and it sits below dataLayer in the
# hierarchy. In the SRC data layer, a data model is synonymous with a source
# system. In the staging data layer, a data model is synonymous with a "stage"
# in the ETL process. In the target data layer, there is just one data model:
# the target data model. In the summary data layer, there is again just one
# data model: the summary data model.
#


class Dataset():

    def __init__(self,
                 dataConf,
                 dmSchemaDesc,
                 datastore,
                 dataLayerID,
                 tableNameMap=None):

        self.dataConf = dataConf

        self.dataLayerID = dataLayerID

        # if we have to TRG schemadesc, we create a "standard" TRG dm instead
        if dmSchemaDesc is None:
            self.dataModelID = 'TRG'
            dmSchemaDesc = {
                'tableSchemas': {}
            }
        else:
            self.dataModelID = dmSchemaDesc['dataModelID']

        self.datastore = datastore

        self.tables = {}

        for tableName in dmSchemaDesc['tableSchemas']:
            srcTableName = None
            if tableNameMap is not None:
                _tableName = tableName[tableName.find("_")+1:]
                _tableName = _tableName[_tableName.find("_")+1:]
                srcTableName = tableNameMap[_tableName]
            if self.dataModelID in ('TRG', 'SUM'):
                table = TrgTable(
                    self.dataConf,
                    dmSchemaDesc['tableSchemas'][tableName],
                    self.datastore,
                    self.dataLayerID,
                    self.dataModelID)
            else:
                table = Table(
                    self.dataConf,
                    dmSchemaDesc['tableSchemas'][tableName],
                    self.datastore,
                    self.dataLayerID,
                    self.dataModelID,
                    srcTableName)
            self.tables[tableName] = table

    def getSqlCreateStatements(self):

        sql = []
        for tName in self.tables:
            sql.append(self.tables[tName].getSqlCreateStatement())
        return sql

    def getSqlDropStatements(self):

        sql = []
        for tName in self.tables:
            sql.append(self.tables[tName].getSqlDropStatement())
        return sql

    def getSqlResetPrimaryKeySequences(self):

        sql = []
        for tName in self.tables:
            sql.append(self.tables[tName].getSqlResetPrimaryKeySequence())
        return sql

    def getListOfTables(self):
        tables = []
        for tableName in self.tables:
            tables.append(tableName)
        return tables

    def getColumnsForTable(self, searchTableName):
        for tableName in self.tables:
            if searchTableName == tableName:
                return self.tables[tableName].columns

    def __str__(self):
        string = '\n' + '  ** ' + self.dataModelID + ' **' + '\n'
        for tableName in self.tables:
            string += str(self.tables[tableName])
        return string


class SrcDataset(Dataset):

    def __init__(self,
                 dataConf,
                 dmSchemaDesc,
                 tableNameMap,
                 datastore,
                 dataLayerID):

        Dataset.__init__(
            self,
            dataConf,
            dmSchemaDesc,
            datastore,
            dataLayerID,
            tableNameMap)

        self.dataConf = dataConf


class EmptyDataset():

    def __init__(self):

        self.tables = {}

    def getSqlDropStatements(self):
        return []

    def getSqlCreateStatements(self):
        return []

    def getListOfTables(self):
        return []

    def getColumnsForTable(self, tableName):
        return None