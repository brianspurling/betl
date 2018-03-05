from . import api


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
            columnCreateStatement = self.columnName + ' SERIAL UNIQUE'
        else:
            columnCreateStatement = self.columnName + ' ' + \
                self.dataType

        return columnCreateStatement

    def getSqlResetPrimaryKeySequence(self, tableName):
        columnResetStatement = None
        if self.isSK:
            seqName = tableName + '_' + self.columnName + '_' + 'key'
            columnResetStatement = 'ALTER SEQUENCE ' + seqName + \
                'RESTART WITH 1'

        return columnResetStatement

    def getSKlookup(self):
        return api.readDataFromCsv('sk_' + self.fkDimension)

    def __str__(self):
        return '      ' + self.columnName + '\n'
