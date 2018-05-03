from .table import Table
from .table import TrgTable

#
# A data model is a collection of tables, and it sits below dataLayer in the
# hierarchy. In the SRC data layer, a data model is synonymous with a source
# system. In the staging data layer, a data model is synonymous with a "stage"
# in the ETL process. In the target data layer, there is just one data model:
# the target data model. In the summary data layer, there is again just one
# data model: the summary data model.
#


class DataModel():

    def __init__(self, conf, dataModelSchemaDesc, datastore, dataLayerID):

        self.conf = conf

        self.dataLayerID = dataLayerID
        self.dataModelID = dataModelSchemaDesc['dataModelID']
        self.datastore = datastore

        self.tables = {}
        self.isSchemaDefined = True

        if (len(dataModelSchemaDesc) == 0):
            self.isSchemaDefined = False

        for tableName in dataModelSchemaDesc['tableSchemas']:
            if self.dataModelID in ('TRG', 'SUM'):
                table = TrgTable(
                    self.conf,
                    dataModelSchemaDesc['tableSchemas'][tableName],
                    self.datastore,
                    self.dataLayerID,
                    self.dataModelID)
            else:
                table = Table(
                    self.conf,
                    dataModelSchemaDesc['tableSchemas'][tableName],
                    self.datastore,
                    self.dataLayerID,
                    self.dataModelID)
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


class SrcDataModel(DataModel):

    # this needs appConf, when the main one doesn't, because this loads up
    # the spreadsheet. but why dont' i need it for files?? maybe I do - not
    # tested  No it's beecause the spreadsheet func is writtend differnetly,
    # because it's the schema func
    def __init__(self, conf, dataModelSchemaDesc, datastore, dataLayerID):

        DataModel.__init__(
            self,
            conf,
            dataModelSchemaDesc,
            datastore,
            dataLayerID)

        self.conf = conf
        # self.srcSysDatastore = \
        #    conf.data.getSrcSysDatastore(dataModelSchemaDesc['dataModelID'])
