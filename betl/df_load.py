from . import api
from . import logger
import pandas as pd
import numpy as np

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
    if scheduler.bulkOrDelta == 'BULK':
        for tableName in trgTables:
            if (trgTables[tableName].getTableType() == 'FACT'):
                if tableName not in nonDefaultStagingTables:
                    JOB_LOG.info(
                        logger.logStepStart('Dropping fact indexes for ' +
                                            tableName))
                    trgTables[tableName].dropIndexes()

    # We'll need the default rows for a bulk load, so let's just hit the
    # spreadsheet once
    defaultRows = {}
    if scheduler.bulkOrDelta == 'BULK':
        worksheets = scheduler.conf.app.DEFAULT_ROW_SRC.worksheets
        # Our defaultRows SS should contain a tab per dimension, each with 1+
        # default rows defined. IDs are defined too - should all be negative
        for wsTitle in worksheets:
            # The worksheet name should be the table name
            defaultRows[wsTitle] = worksheets[wsTitle].get_all_records()

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
                              bulkOrDelta=scheduler.bulkOrDelta,
                              defaultRows=defaultRows,
                              dataIO=scheduler.dataIO)


def loadTable(table, bulkOrDelta, defaultRows, dataIO):

    tableType = table.getTableType()

    if bulkOrDelta == 'BULK' and tableType == 'DIMENSION':
        bulkLoadDimension(table=table, defaultRows=defaultRows, dataIO=dataIO)
    elif bulkOrDelta == 'BULK' and tableType == 'FACT':
        bulkLoadFact(table=table, dataIO=dataIO)
    elif bulkOrDelta == 'DELTA' and tableType == 'DIMENSION':
        bulkLoadDimension(table=table)
    elif bulkOrDelta == 'DELTA' and tableType == 'FACT':
        deltaLoadFact(table=table)


def bulkLoadDimension(table, defaultRows, dataIO):

    # We assume that the final step in the TRANSFORM stage created a
    # csv file trg_<tableName>.csv
    # TODO: put in a decent feedback to developer if they didn't create
    # the right table. Start by raising custom error inside readData
    df = api.readData('trg_' + table.tableName, 'STG')

    # Because it's a bulk load, clear out the data (which also
    # restarts the SK sequences). Note, the indexes have already been
    # removed
    JOB_LOG.info(
        logger.logStepStart('Truncating ' + table.tableName))
    table.truncateTable()
    dataIO.truncateFile(filename=table.tableName,
                        dataLayerID=table.dataLayerID)

    # We can append rows, because we just truncated. This way, append
    # guarantees we error if we don't load all the required columns
    api.writeData(df, table.tableName, 'TRG', 'append')

    # Put the indexes back on
    JOB_LOG.info(
        logger.logStepStart('Creating indexes for ' + table.tableName))
    table.createIndexes()

    del df

    # Add the default rows
    JOB_LOG.info(
        logger.logStepStart('Generating default rows for ' + table.tableName))

    # The app does not have to specify default rows if it doesn't want to
    if table.tableName in defaultRows:
        df = pd.DataFrame.from_dict(defaultRows[table.tableName])
        # blank values in the spreadsheet come through as empty strings, but
        # should be NULL in the DB
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        JOB_LOG.info(logger.logStepEnd(df))

        # We skip the API for this call because we're telling dataIO that this
        # is a defaultRows write, so it knows to remove the SK for the file
        # write
        dataIO.writeData(df, table.tableName, 'TRG', 'append',
                         writingDefaultRows=True)

    # We will need the SKs we just created to write the facts later, so
    # pull the sk/nks back out (this func writes them to a csv file)
    api.getSKMapping(table.tableName,
                     table.colNames_NKs,
                     table.surrogateKeyColName)


def bulkLoadFact(table, dataIO):
    df_ft = api.readData('trg_' + table.tableName, 'STG')
    for column in table.columns:
        if column.isFK:
            df_ft = api.mergeFactWithSks(df_ft, column)

    # Order the df's columns - the df doesn't hold the SK
    df_ft = df_ft[table.colNames_withoutSK]

    # Because it's a bulk load, clear out the data (which also
    # restarts the SK sequences). Note, the indexes have already been
    # removed
    JOB_LOG.info(
        logger.logStepStart('Truncating ' + table.tableName))
    table.truncateTable()
    dataIO.truncateFile(filename=table.tableName,
                        dataLayerID=table.dataLayerID)
    # We can append rows, because, as we're running a bulk load, we will
    # have just cleared out the TRG model and created. This way, append
    # guarantees we error if we don't load all the required columns
    api.writeData(df_ft, table.tableName, 'TRG', 'append')

    # Put the indexes back on
    JOB_LOG.info(
        logger.logStepStart('Creating indexes for ' + table.tableName))
    table.createIndexes()


def deltaLoadDimension(self):
    raise ValueError("Code not yet written for delta dimension loads")


def deltaLoadFact(self):
    raise ValueError("Code not yet written for delta fact loads")
