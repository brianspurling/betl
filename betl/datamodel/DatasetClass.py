from .TableClass import Table

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
                 conf,
                 datasetSchemaDesc,
                 dataLayerID):

        self.CONF = conf

        self.dataLayerID = dataLayerID

        self.datasetID = datasetSchemaDesc['datasetID']

        self.tables = {}

        for tableName in datasetSchemaDesc['tableSchemas']:
            self.tables[tableName] = Table(
                conf=self.CONF,
                tableSchema=datasetSchemaDesc['tableSchemas'][tableName],
                dataLayerID=self.dataLayerID,
                datasetID=self.datasetID)

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
        string = '\n' + '  ** ' + self.datasetID + ' **' + '\n'
        for tableName in self.tables:
            string += str(self.tables[tableName])
        return string


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
