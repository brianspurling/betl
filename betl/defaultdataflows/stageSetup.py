def logBETLStart(**kwargs):
    kwargs['conf'].log('logBETLStart', test=kwargs['test'])


def cleanTableName(tableName_src):
    tableName = tableName_src
    tableName = tableName.replace(' ', '_')
    tableName = tableName.replace('(', '')
    tableName = tableName.replace(')', '')
    tableName = tableName.replace('-', '')
    tableName = tableName.lower()
    return tableName
