class Column():

    def __init__(self, columnSchema):
        self.tableName = columnSchema['tableName']
        self.columnName = columnSchema['columnName']
        self.dataType = columnSchema['dataType']
        self.fkDimension = columnSchema['fkDimension']

        self.isSK = False
        self.isNK = False
        self.isFK = False

        if columnSchema['columnType'].upper() == 'FOREIGN KEY':
            self.isFK = True
        elif columnSchema['columnType'].upper() == 'NATURAL KEY':
            self.isNK = True
        elif columnSchema['columnType'].upper() == 'SURROGATE KEY':
            self.isSK = True

    def getSqlCreateStatement(self):
        columnCreateStatement = ''
        if self.isSK:
            columnCreateStatement = '"' + self.columnName + '"' + \
                ' SERIAL UNIQUE'
        else:
            columnCreateStatement = '"' + self.columnName + '"' + ' ' + \
                self.dataType

        return columnCreateStatement

    def getSqlResetPrimaryKeySequence(self, tableName):
        columnResetStatement = None
        if self.isSK:
            seqName = tableName + '_' + self.columnName + '_' + 'key'
            columnResetStatement = 'ALTER SEQUENCE ' + seqName + \
                'RESTART WITH 1'

        return columnResetStatement

    def getSqlDropIndexStatement(self):
        return ('DROP INDEX IF EXISTS ' +
                self.tableName + '_' + self.columnName + '_key')

    def getSqlDropForeignKeyStatement(self):
        return ('ALTER TABLE ' + self.tableName + ' ' +
                'DROP CONSTRAINT IF EXISTS ' +
                self.tableName + '_' + self.columnName + '_key')

    def getSqlCreateIndexStatements(self):
        sqlStatements = []

        if self.isSK or self.isFK:
            unique = ''
            if self.isSK:
                unique = 'UNIQUE'
            sqlStatements.append(
                'CREATE ' + unique + ' INDEX IF NOT EXISTS ' +
                self.tableName + '_' + self.columnName + '_key' +
                ' ON ' + self.tableName + ' (' + self.columnName + ')')

        if self.isFK:
            fkDimCol = self.fkDimension[3:] + '_id'
            sqlStatements.append(
                'ALTER TABLE ' + self.tableName + ' ' +
                'ADD CONSTRAINT ' +
                self.tableName + '_' + self.columnName + '_key ' +
                'FOREIGN KEY (' + self.columnName + ')' +
                'REFERENCES ' + self.fkDimension + '(' + fkDimCol + ')')

        return sqlStatements

    def __str__(self):
        return '      ' + self.columnName + '\n'
