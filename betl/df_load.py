from . import api
from . import logger

JOB_LOG = logger.getJobLog()


#
# A default load process. Bulk is obvious and as you would expect
# Delta deals with SCD et al
# This function assumes that dm_a_dimension is loaded from a csv file
# called dm_a_dimension.csv. If that isn't the case, pass in to
# nonDefaultStagingTables a key,value pair of <dimension name>,<staging csv>
#
def defaultLoad(scheduler):

    trgLayer = scheduler.logicalDataModels['TRG']

    trgTables = trgLayer.dataModels['TRG'].tables
    nonDefaultStagingTables = \
        scheduler.conf.schedule.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    # If it's a bulk load, drop the indexes to speed up writing. We do this
    # here, because we need to drop fact indexes first (or, to be precise,
    # the facts' foreign key constraints, because the dim ID indexes cant be
    # dropped until the FKs that point to them are gone)
    for tableName in trgTables:
        if (trgTables[tableName].getTableType() == 'FACT'):
            if tableName not in nonDefaultStagingTables:
                JOB_LOG.info(
                    logger.logStepStart('Dropping indexes for ' + tableName))
                trgTables[tableName].dropIndexes()

    # We must load the dimensions before the facts!
    loadSequence = []
    if (scheduler.conf.exe.RUN_DM_LOAD):
        loadSequence.append('DIMENSION')
    if (scheduler.conf.exe.RUN_FT_LOAD):
        loadSequence.append('FACT')

    for tableType in loadSequence:
        for tableName in trgTables:
            if (trgTables[tableName].getTableType() == tableType):
                if tableName not in nonDefaultStagingTables:
                    loadTable(table=trgTables[tableName],
                              bulkOrDelta=scheduler.bulkOrDelta)


def loadTable(table, bulkOrDelta):

    tableType = table.getTableType()

    if bulkOrDelta == 'BULK' and tableType == 'DIMENSION':
        bulkLoadDimension(table=table)
    elif bulkOrDelta == 'BULK' and tableType == 'FACT':
        bulkLoadFact(table=table)
    elif bulkOrDelta == 'BULK' and tableType == 'DIMENSION':
        bulkLoadDimension(table=table)
    elif bulkOrDelta == 'DELTA' and tableType == 'FACT':
        deltaLoadFact(table=table)


def bulkLoadDimension(table):

    # We assume that the final step in the TRANSFORM stage created a
    # csv file trg_<tableName>.csv
    # TODO: put in a decent feedback to developer if they didn't create
    # the right table. Start by raising custom error inside readDataFromCsv
    df = api.readDataFromCsv('trg_' + table.tableName)

    # Because it's a bulk load, clear out the data (which also
    # restarts the SK sequences). Note, the indexes have already been
    # removed
    table.truncateTable()

    # We can append rows, because we just truncated. This way, append
    # guarantees we error if we don't load all the required columns
    api.writeDataToTrgDB(df, table.tableName, if_exists='append')

    # Put the indexes back on
    JOB_LOG.info(
        logger.logStepStart('Creating indexes for ' + table.tableName))
    table.createIndexes()

    del df

    # We will need the SKs we just created to write the facts later, so
    # pull the sk/nks back out (this func writes them to a csv file)
    api.getSKMapping(table.tableName,
                     table.colNames_NKs,
                     table.surrogateKeyColName)


def bulkLoadFact(table):
    df_ft = api.readDataFromCsv('trg_' + table.tableName)
    for column in table.columns:
        if column.isFK:
            df_ft = api.mergeFactWithSks(df_ft, column)

    # Order the df's columns - the df doesn't hold the SK
    df_ft = df_ft[table.colNames_withoutSK]

    # we remove indexes first to speed up writing
    JOB_LOG.info(
        logger.logStepStart('Dropping indexes for ' + table.tableName))
    table.dropIndexes()

    # We can append rows, because, as we're running a bulk load, we will
    # have just cleared out the TRG model and created. This way, append
    # guarantees we error if we don't load all the required columns
    api.writeDataToTrgDB(df_ft, table.tableName, if_exists='append')

    # Put the indexes back on
    JOB_LOG.info(
        logger.logStepStart('Creating indexes for ' + table.tableName))
    table.createIndexes()


def deltaLoadDimension(self):
    raise ValueError("Code not yet written for delta dimension loads")


def deltaLoadFact(self):
    raise ValueError("Code not yet written for delta fact loads")
