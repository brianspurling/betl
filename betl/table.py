from .column import Column
from . import df_load


class Table():

    def __init__(self, tableSchema):

        self.tableName = tableSchema['tableName'].lower()

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

    def getSqlTruncateStatement(self):

        return 'TRUNCATE ' + self.tableName + ' RESTART IDENTITY'

    def getSqlResetPrimaryKeySequence(self):

        return self.surrogateKeyColumn.getSqlResetPrimaryKeySequence()

    def loadTableToTrgModel(self):

        df_load.loadTable(self)

    def __str__(self):

        string = '\n' + '    ' + self.tableName + '\n'
        string += ''.join(map(str, self.columns))
        return string


class TrgTable(Table):

    def __init__(self, tableSchema):

        Table.__init__(self, tableSchema)

        self.tableType = ''
        if self.tableName[0:3] == 'ft_':
            self.tableType = 'FACT'
        elif self.tableName[0:3] == 'dm_':
            self.tableType = 'DIMENSION'
        else:
            raise ValueError("Can't determine table type for " +
                             self.tableName)

    def getTableType(self):

        tableType = 'UNKNOWN'

        # TODO is this reliable enough?
        if self.tableName[:3] == 'dm_':
            tableType = 'DIMENSION'
        elif self.tableName[:3] == 'ft_':
            tableType = 'FACT'
        elif self.tableName[:3] == 'su_':
            tableType = 'SUMMARY'

        return tableType
