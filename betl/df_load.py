from . import api as betl

import pandas as pd
import numpy as np


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

    if scheduler.bulkOrDelta == 'BULK':
        dfl = betl.DataFlow(
            desc="If it's a bulk load, drop the indexes to speed up " +
                 "writing. We do this here, because we need to drop " +
                 "fact indexes first (or, to be precise, the facts' " +
                 "foreign key constraints, because the dim ID indexes " +
                 " cant be dropped until the FKs that point to them are gone)")
        for tableName in trgTables:
            if (trgTables[tableName].getTableType() == 'FACT'):
                if tableName not in nonDefaultStagingTables:
                    for sql in trgTables[tableName].getSqlDropIndexes():
                        dfl.customSQL(
                            sql,
                            dataLayer='TRG',
                            desc='Dropping fact indexes for ' + tableName)
        dfl.close()

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
                              conf=scheduler.conf)


def loadTable(table, bulkOrDelta, conf):

    tableType = table.getTableType()

    if bulkOrDelta == 'BULK' and tableType == 'DIMENSION':
        bulkLoadDimension(conf=conf, table=table)
    elif bulkOrDelta == 'BULK' and tableType == 'FACT':
        bulkLoadFact(conf=conf, table=table)
    elif bulkOrDelta == 'DELTA' and tableType == 'DIMENSION':
        bulkLoadDimension(conf=conf, table=table)
    elif bulkOrDelta == 'DELTA' and tableType == 'FACT':
        deltaLoadFact(conf=conf, table=table)


def bulkLoadDimension(conf, table):

    dfl = betl.DataFlow(
        desc='Loading dimension: ' + table.tableName)

    # DATA

    dfl.truncate(
        dataset=table.tableName,
        dataLayerID='TRG',
        forceDBWrite=True,
        desc='Because it is a bulk load, clear out the data (which also ' +
             'restarts the SK sequences)')

    dataset = 'trg_' + table.tableName
    dfl.read(
        tableName=dataset,
        dataLayer='STG',
        desc='Read the data we are going to load (the default load ' +
             'assumes that the final step in the TRANSFORM ' +
             'stage created a csv file trg_' + table.tableName + ')')

    dfl.write(
        dataset=dataset,
        targetTableName=table.tableName,
        dataLayerID='TRG',
        forceDBWrite=True,
        writingDefaultRows=True,
        desc='Load data into the target model for ' + table.tableName,
        keepDataflowOpen=True)

    # INDEXES

    for sql in table.getSqlCreateIndexes():
        dfl.customSQL(
            sql,
            dataLayer='TRG',
            desc='Creating index for ' + table.tableName)

    # DEFAULT ROWS

    # Our defaultRows SS should contain a tab per dimension, each with 1+
    # default rows defined. IDs are defined too - should all be negative
    defaultRows = {}
    worksheets = conf.app.DEFAULT_ROW_SRC.worksheets
    for wsTitle in worksheets:
        defaultRows[wsTitle] = worksheets[wsTitle].get_all_records()

    # Default rows are not cumpulsory
    if table.tableName in defaultRows:
        # Blank values in the spreadsheet come through as empty strings, but
        # should be NULL in the DB
        df = pd.DataFrame.from_dict(defaultRows[table.tableName])
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        dfl.createDataset(
            dataset=table.tableName + '_defaultRows',
            data=df)

        dfl.write(
            dataset=table.tableName + '_defaultRows',
            targetTableName=table.tableName,
            dataLayerID='TRG',
            forceDBWrite=True,
            append_or_replace='append',
            writingDefaultRows=True,
            desc='Adding default rows to ' + table.tableName,
            keepDataflowOpen=True)

    # RETRIEVE SK/NK MAPPING (FOR LATER)

    dfl.read(
        tableName=table.tableName,
        dataLayer='TRG',
        forceDBRead=True,
        desc='The SKs were generated as we wrote to the DB. We will need ' +
             'these SKs (and their corresponding NKs) when we load the fact ' +
             'table (later), so we pull the sk/nks mapping back out now)')

    dfl.dropColumns(
        dataset=table.tableName,
        colsToKeep=[table.surrogateKeyColName] + table.colNames_NKs,
        desc='Drop all cols except SK & NKs')

    dfl.renameColumns(
        dataset=table.tableName,
        columns={table.surrogateKeyColName: 'sk'},
        desc='Rename the SK column to "sk"')

    dfl.addColumns(
        dataset=table.tableName,
        columns={'nk': concatenateNKs},
        desc='Concatenate the NK columns into a single "nk" column')

    dfl.dropColumns(
        dataset=table.tableName,
        colsToKeep=['sk', 'nk'],
        desc='Drop all cols except the sk col and the new nk col')

    dfl.write(
        dataset=table.tableName,
        targetTableName='sk_' + table.tableName,
        dataLayerID='STG')


def concatenateNKs(row):
    nks = []
    for col in row.columns.values:
        if col == 'sk':
            continue
        if col == 'nk':
            continue
        else:
            nks.append(str(row[col]))
    return '_'.join(nks)


def bulkLoadFact(conf, table):

    dfl = betl.DataFlow(
        desc='Loading fact: ' + table.tableName)

    # READ DATA

    dfl.truncate(
        dataset=table.tableName,
        dataLayerID='TRG',
        forceDBWrite=True,
        desc='Because it is a bulk load, clear out the data (which also ' +
             'restarts the SK sequences)')

    ftStgTableName = 'trg_' + table.tableName
    dfl.read(
        tableName=ftStgTableName,
        dataLayer='STG',
        desc='Read the data we are going to load (the default load ' +
             'assumes that the final step in the TRANSFORM ' +
             'stage created a csv file trg_' + table.tableName + ')')

    # SK/NK MAPPINGS

    for column in table.columns:
        if column.isFK:
            keyMapTableName = 'sk_' + column.fkDimension
            dfl.read(
                tableName=keyMapTableName,
                dataLayer='STG',
                desc='Read the SK/NK mapping for column ' + column.columnName)

            nkColName = column.columnName.replace('fk_', 'nk_')

            dfl.renameColumns(
                dataset=keyMapTableName,
                columns={
                    'sk': column.columnName,
                    'nk': nkColName},
                desc='Rename the columns of the ' + column.fkDimension + ' ' +
                     'SK/NK mapping to match the fact table column names')

            dfl.join(
                datasets=[keyMapTableName, ftStgTableName],
                targetDataset=table.tableName,
                joinCol=nkColName,
                keepCols=['latest_delta_load_operation', 'data_quality_score'],
                how='left',
                desc="Merging dim's SK with fact for column " +
                     column.columnName)

            dfl.setNulls(
                dataset=table.tableName,
                columns={column.columnName: -1},
                desc='Assigning all missing rows to default -1 row')

            dfl.dropColumns(
                dataset=table.tableName,
                colsToDrop=['nkColName'],
                desc='dropping unneeded column: ' + nkColName)

    # WRITE DATA

    dfl.write(
        dataset=table.tableName,
        targetTableName=table.tableName,
        dataLayerID='TRG',
        keepDataflowOpen=True)

    # INDEXES

    for sql in table.getSqlCreateIndexes():
        dfl.customSQL(
            sql,
            dataLayer='TRG',
            desc='Creating index for ' + table.tableName)

    dfl.close()


def deltaLoadDimension(conf, table):
    raise ValueError("Code not yet written for delta dimension loads")


def deltaLoadFact(conf, table):
    raise ValueError("Code not yet written for delta fact loads")
