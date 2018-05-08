from .column import Column
from . import df_load


class Table():

    def __init__(self, conf, tableSchema, datastore, dataLayerID, dataModelID):

        self.conf = conf
        self.dataLayerID = dataLayerID
        self.dataModelID = dataModelID
        self.tableName = tableSchema['tableName'].lower()
        self.datastore = datastore

        self.columns = []
        self.colNames = []

        self.surrogateKeyColName = ''
        self.surrogateKeyColumn = None

        self.colNames_withoutSK = []
        self.colNames_NKs = []
        self.colNames_FKs = []
        self.colNames_withoutNKs = []

        for columnSchema in tableSchema['columnSchemas']:
            col = Column(tableSchema['columnSchemas'][columnSchema])
            self.columns.append(col)
            self.colNames.append(col.columnName)

            if col.isNK:
                self.colNames_NKs.append(col.columnName)
            else:
                self.colNames_withoutNKs.append(col.columnName)

            if col.isSK:
                self.surrogateKeyColName = col.columnName
                self.surrogateKeyColumn = col
            else:
                self.colNames_withoutSK.append(col.columnName)

            if col.isFK:
                self.colNames_FKs.append(col.columnName)

        # The fact tables get a FK to the audit dimension
        if self.getTableType() == 'FACT':
            columnSchema = {
                'tableName':   self.tableName,
                'columnName':  'fk_audit',
                'dataType':    'INTEGER',
                'columnType':  'Foreign key',
                'fkDimension': 'dm_audit'
            }
            col = Column(columnSchema)
            self.columns.append(col)
            self.colNames.append(col.columnName)
            self.colNames_withoutNKs.append(col.columnName)
            self.colNames_withoutSK.append(col.columnName)
            self.colNames_FKs.append(col.columnName)

    def getSqlCreateStatement(self):

        tableCreateStatement = 'CREATE TABLE ' + self.tableName + ' ('

        colsCreateStatements = []
        for columnObject in self.columns:
            colsCreateStatements.append(columnObject.getSqlCreateStatement())

        # For all tables except fact and summary tables in the TRG Layer,
        # and DM_AUDIT, we add audit columns
        tableType = self.getTableType()
        if tableType != 'FACT' and self.tableName != 'dm_audit':
            for i, auditColRow in self.conf.auditColumns.iterrows():
                colsCreateStatements.append(
                    auditColRow['colNames'] +
                    ' ' +
                    auditColRow['dataType'])

        tableCreateStatement += ', '.join(colsCreateStatements)

        tableCreateStatement += ')'

        return tableCreateStatement

    def getSqlDropStatement(self):

        tableDropStatement = 'DROP TABLE IF EXISTS ' + self.tableName

        return tableDropStatement

    def getTableType(self):

        tableType = 'UNKNOWN'

        if self.dataLayerID not in ('TRG', 'SUM'):
            tableType = 'Not TRG Layer'
        elif self.tableName[:3] == 'dm_':
            tableType = 'DIMENSION'
        elif self.tableName[:3] == 'ft_':
            tableType = 'FACT'
        elif self.tableName[:3] == 'su_':
            tableType = 'SUMMARY'
        else:
            raise ValueError("Can't determine table type for " +
                             self.tableName)

        return tableType

    def __str__(self):

        string = '\n' + '    ' + self.tableName + '\n'
        string += ''.join(map(str, self.columns))
        return string


# TRG tables are any table in the TRG database, i.e. datalayers TRG & SUM
class TrgTable(Table):

    def __init__(self, conf, tableSchema, datastore, dataLayerID, dataModelID):

        Table.__init__(self, conf, tableSchema,
                       datastore, dataLayerID, dataModelID)

    def getSqlDropIndexes(self):

        sqlStatements = []
        for col in self.columns:
            if col.isSK or col.isFK:
                # We must drop a column's foreign key before its index
                sqlStatements.append(col.getSqlDropForeignKeyStatement())
                sqlStatements.append(col.getSqlDropIndexStatement())

        return sqlStatements

    def getSqlCreateIndexes(self):

        return self.getSqlCreateIndexStatements()

    def getSqlResetPrimaryKeySequence(self):

        return self.surrogateKeyColumn.getSqlResetPrimaryKeySequence()

    def getSqlCreateIndexStatements(self):
        sqlStatements = []
        for col in self.columns:
            if col.isSK or col.isFK:
                sqlStatements.extend(col.getSqlCreateIndexStatements())
        return sqlStatements

    def loadTableToTrgModel(self):

        df_load.loadTable(self)
