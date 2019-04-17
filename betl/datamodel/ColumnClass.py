class Column():

    def __init__(self, columnSchema):

        if 'schema' in columnSchema:
            self.schema = columnSchema['schema']
        else:
            self.schema = None

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
            columnCreateStatement = '"' + str(self.columnName) + '"' + \
                ' SERIAL UNIQUE'
        else:
            columnCreateStatement = '"' + str(self.columnName) + '"' + ' ' + \
                self.dataType

        return columnCreateStatement

    def getSqlResetPrimaryKeySequence(self, tableName):
        schema = ''
        if self.schema is not None:
            schema = self.schema + '.'

        columnResetStatement = None
        if self.isSK:
            seqName = schema + tableName + '_' + str(self.columnName) + '_' + 'key'
            columnResetStatement = 'ALTER SEQUENCE ' + seqName + \
                'RESTART WITH 1'

        return columnResetStatement

    def getSqlDropIndexStatement(self):
        schema = ''
        if self.schema is not None:
            schema = self.schema + '.'

        return ('DROP INDEX IF EXISTS ' + schema +
                self.tableName + '_' + str(self.columnName) + '_key')

    def getSqlDropForeignKeyStatement(self):
        schema = ''
        if self.schema is not None:
            schema = self.schema + '.'
        return ('ALTER TABLE ' + schema + self.tableName + ' ' +
                'DROP CONSTRAINT IF EXISTS ' +
                self.tableName + '_' + str(self.columnName) + '_key')

    def getSqlCreateIndexStatements(self):
        sqlStatements = []
        schema = ''
        if self.schema is not None:
            schema = self.schema + '.'

        if self.isSK or self.isFK:
            unique = ''
            if self.isSK:
                unique = 'UNIQUE'

            sqlStatements.append(
                'CREATE ' + unique + ' INDEX IF NOT EXISTS ' + schema +
                self.tableName + '_' + str(self.columnName) + '_key' +
                ' ON ' + self.tableName + ' (' + str(self.columnName) + ')')

        if self.isFK:
            fkDimCol = self.fkDimension[3:] + '_id'
            sqlStatements.append(
                'ALTER TABLE ' + self.tableName + ' ' +
                'ADD CONSTRAINT ' + schema +
                self.tableName + '_' + str(self.columnName) + '_key ' +
                'FOREIGN KEY (' + str(self.columnName) + ')' +
                'REFERENCES ' + self.fkDimension + '(' + fkDimCol + ')')

        return sqlStatements

    def __str__(self):
        return '      ' + str(self.columnName) + '\n'
