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

    trgLayer = scheduler.conf.getLogicalDataModel('TRG')

    trgTables = trgLayer.dataModels['TRG'].tables
    nonDefaultStagingTables = \
        scheduler.conf.schedule.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    # We must load the dimensions before the facts!
    loadSequence = []
    if (scheduler.conf.exe.RUN_DM_LOAD):
        loadSequence.append('DIMENSION')
    if (scheduler.conf.exe.RUN_FT_LOAD):
        loadSequence.append('FACT')

    if scheduler.bulkOrDelta == 'BULK':

        # DROP INDEXES

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

        # GET ALL DEFAULT ROWS

        # Our defaultRows SS should contain a tab per dimension, each with 1+
        # default rows defined. IDs are defined too - should all be negative
        defaultRows = {}
        worksheets = scheduler.conf.data.getDefaultRowsDatastore().worksheets
        for wsTitle in worksheets:
            defaultRows[wsTitle] = worksheets[wsTitle].get_all_records()

        for dimOrFactLoad in loadSequence:
            for tableName in trgTables:
                tableType = trgTables[tableName].getTableType()
                if (tableType == dimOrFactLoad):
                    if tableName not in nonDefaultStagingTables:
                        bulkLoadTable(table=trgTables[tableName],
                                      tableType=tableType,
                                      defaultRows=defaultRows,
                                      conf=scheduler.conf)

    if scheduler.bulkOrDelta == 'DELTA':
        for tableType in loadSequence:
            for tableName in trgTables:
                tableType = trgTables[tableName].getTableType()
                if (tableType == tableType):
                    if tableName not in nonDefaultStagingTables:
                        deltaLoadTable(table=trgTables[tableName],
                                       tableType=tableType,
                                       conf=scheduler.conf)


def bulkLoadTable(table, tableType, defaultRows, conf):
    if tableType == 'DIMENSION':
        bulkLoadDimension(conf=conf, defaultRows=defaultRows, table=table)
    elif tableType == 'FACT':
        bulkLoadFact(conf=conf, table=table)


def deltaLoadTable(table, tableType, conf):
    if tableType == 'DIMENSION':
        deltaLoadDimension(conf=conf, table=table)
    elif tableType == 'FACT':
        deltaLoadFact(conf=conf, table=table)


def bulkLoadDimension(conf, defaultRows, table):
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
        desc='Read the data we are going to load to TRG (from file trg_' +
             table.tableName + ')')

    dfl.write(
        dataset=dataset,
        targetTableName=table.tableName,
        dataLayerID='TRG',
        forceDBWrite=True,
        append_or_replace='append',  # stops it altering table & removing SK!
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

    if table.tableName in defaultRows:  # Default rows are not compulsory

        # Blank values in the spreadsheet come through as empty strings, but
        # should be NULL in the DB
        # TODO: this should be a dataflow op, part of effort to tidy up the
        # apply functions
        df = pd.DataFrame.from_dict(defaultRows[table.tableName])
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        dfl.createDataset(
            dataset=table.tableName + '_defaultRows',
            data=df,
            desc='Loading the default rows into the dataflow')

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
        desc='Drop all cols except SK & NKs (including audit cols)',
        dropAuditCols=True)

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
    # TODO not sure why row is a series here, this is a temp solution
    rowDict = row.to_dict()
    nks = []
    for col in rowDict:
        if col in ('sk', 'nk'):
            continue
        else:
            nks.append(str(rowDict[col]))
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

    dfl.read(
        tableName='trg_' + table.tableName,
        dataLayer='STG',
        targetDataset=table.tableName,
        desc='Read the data we are going to load to TRG (from file ' +
             'trg_' + table.tableName + ')')

    dfl.createAuditNKs(
        dataset=table.tableName,
        desc='Collapse the audit columns into their NK')

    dfl.write(
        dataset=table.tableName,
        targetTableName='trg_' + table.tableName,
        dataLayerID='STG',
        desc='Write it back to ' + table.tableName + ' for debug',
        keepDataflowOpen=True)

    # SK/NK MAPPINGS

    for column in table.columns:
        if column.isFK:
            keyMapTableName = 'sk_' + column.fkDimension
            dfl.read(
                tableName=keyMapTableName,
                targetDataset=keyMapTableName + '.' + column.columnName,
                dataLayer='STG',
                desc='Read the SK/NK mapping for column ' + column.columnName)

            nkColName = column.columnName.replace('fk_', 'nk_')

            dfl.renameColumns(
                dataset=keyMapTableName + '.' + column.columnName,
                columns={
                    'sk': column.columnName,
                    'nk': nkColName},
                desc='Rename the columns of the ' + column.fkDimension + ' ' +
                     'SK/NK mapping to match the fact table column names')

            dfl.join(
                datasets=[
                    table.tableName,
                    keyMapTableName + '.' + column.columnName],
                targetDataset=table.tableName,
                joinCol=nkColName,
                how='left',
                desc="Merging dim's SK with fact for column " +
                     column.columnName)

            dfl.setNulls(
                dataset=table.tableName,
                columns={column.columnName: -1},
                desc='Assigning all missing rows to default -1 row')

            dfl.dropColumns(
                dataset=table.tableName,
                colsToDrop=[nkColName],
                desc='Dropping the natural key column: ' + nkColName)

    # WRITE DATA

    dfl.write(
        dataset=table.tableName,
        targetTableName=table.tableName,
        dataLayerID='TRG',
        append_or_replace='append',  # stops it altering table & removing SK!
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
