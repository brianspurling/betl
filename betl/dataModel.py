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

    def __init__(self, dataModelSchema):
        self.dataModelID = dataModelSchema['dataModelID']
        self.tables = {}
        self.isSchemaDefined = True

        if (len(dataModelSchema) == 0):
            self.isSchemaDefined = False

        for tableName in dataModelSchema['tableSchemas']:
            if self.dataModelID == 'TRG':
                table = TrgTable(dataModelSchema['tableSchemas'][tableName])
            else:
                table = Table(dataModelSchema['tableSchemas'][tableName])
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

    def getSqlTruncateStatements(self, truncDims=True, truncFacts=True):

        sql = []
        for tName in self.tables:
            if ((self.tables[tName].tableType == 'DIMENSION' and truncDims)
                    or (self.tables[tName].tableType == 'FACT'
                        and truncFacts)):
                sql.append(self.tables[tName].getSqlTruncateStatement())
        return sql

    def getSqlResetPrimaryKeySequences(self):

        sql = []
        for tName in self.tables:
            sql.append(self.tables[tName].getSqlResetPrimaryKeySequence())
        return sql

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
    def __init__(self, dataModelSchema, conf):

        DataModel.__init__(self, dataModelSchema)

        self.srcSysDatastore = \
            conf.app.SRC_SYSTEMS[dataModelSchema['dataModelID']]
