from .column import Column
from . import df_load


class Table():

    def __init__(self, tableSchema, datastore, dataLayerID, dataModelID):

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

    def getSqlCreateStatement(self):

        tableCreateStatement = 'CREATE TABLE ' + self.tableName + ' ('

        colsCreateStatements = []
        for columnObject in self.columns:
            colsCreateStatements.append(columnObject.getSqlCreateStatement())

        tableCreateStatement += ', '.join(colsCreateStatements)

        tableCreateStatement += ')'

        return tableCreateStatement

    def getSqlDropStatement(self):

        tableDropStatement = 'DROP TABLE IF EXISTS ' + self.tableName

        return tableDropStatement

    def truncateTable(self):

        truncateStatement = 'TRUNCATE ' + self.tableName + ' RESTART IDENTITY'
        trgDbCursor = self.datastore.cursor()
        trgDbCursor.execute(truncateStatement)
        self.datastore.commit()

    def __str__(self):

        string = '\n' + '    ' + self.tableName + '\n'
        string += ''.join(map(str, self.columns))
        return string


# TRG tables are any table in the TRG database, i.e. datalayers TRG & SUM
class TrgTable(Table):

    def __init__(self, tableSchema, datastore, dataLayerID, dataModelID):

        Table.__init__(self, tableSchema, datastore, dataLayerID, dataModelID)

    def getTableType(self):

        tableType = 'UNKNOWN'

        if self.tableName[:3] == 'dm_':
            tableType = 'DIMENSION'
        elif self.tableName[:3] == 'ft_':
            tableType = 'FACT'
        elif self.tableName[:3] == 'su_':
            tableType = 'SUMMARY'
        else:
            raise ValueError("Can't determine table type for " +
                             self.tableName)

        return tableType

    def dropIndexes(self):

        dropStatements = self.getSqlDropIndexStatements()

        dbCursor = self.datastore.cursor()
        for dropStatement in dropStatements:
            dbCursor.execute(dropStatement)
            self.datastore.commit()

    def createIndexes(self):

        createStatements = self.getSqlCreateIndexStatements()

        dbCursor = self.datastore.cursor()
        for createStatement in createStatements:
            dbCursor.execute(createStatement)
            self.datastore.commit()

    def getSqlResetPrimaryKeySequence(self):

        return self.surrogateKeyColumn.getSqlResetPrimaryKeySequence()

    def getSqlDropIndexStatements(self):

        sqlStatements = []
        for col in self.columns:
            if col.isSK or col.isFK:
                # We must drop a column's foreign key before its index
                sqlStatements.append(col.getSqlDropForeignKeyStatement())
                sqlStatements.append(col.getSqlDropIndexStatement())

        return sqlStatements

    def getSqlCreateIndexStatements(self):
        sqlStatements = []
        for col in self.columns:
            if col.isSK or col.isFK:
                sqlStatements.extend(col.getSqlCreateIndexStatements())
        return sqlStatements

    def loadTableToTrgModel(self):

        df_load.loadTable(self)
