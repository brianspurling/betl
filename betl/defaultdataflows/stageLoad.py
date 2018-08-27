import pandas as pd
import numpy as np


#
# A default load process. Bulk is obvious and as you would expect
# Delta deals with SCD et al
# This function assumes that dm_a_dimension is loaded from a csv file
# called dm_a_dimension.csv. If that isn't the case, pass in to
# nonDefaultStagingTables a key,value pair of <dimension name>,<staging csv>
#
# TODO: need separate functions for dims and facts, otherwise the whole thing
# has to rerun
def defaultLoad(betl):

    bseLayer = betl.CONF.DATA.getDataLayerLogicalSchema('BSE')
    sumLayer = betl.CONF.DATA.getDataLayerLogicalSchema('SUM')

    bseTables = bseLayer.datasets['BSE'].tables
    sumTables = sumLayer.datasets['SUM'].tables

    nonDefaultBSETables = \
        betl.CONF.SCHEDULE.BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    # We must load the dimensions before the facts!
    loadSequence = []
    if (betl.CONF.EXE.RUN_DM_LOAD):
        loadSequence.append('DIMENSION')
    if (betl.CONF.EXE.RUN_FT_LOAD):
        loadSequence.append('FACT')

    if betl.CONF.EXE.BULK_OR_DELTA == 'BULK':

        # DROP INDEXES

        bseAndSumTbls = {**bseTables, **sumTables}

        dfl = betl.DataFlow(
            desc="If it's a bulk load, drop the indexes to speed up " +
                 "writing. We do this here, because we need to drop " +
                 "fact indexes first (or, to be precise, the facts' " +
                 "foreign key constraints, because the dim ID indexes " +
                 " cant be dropped until the FKs that point to them are gone)")
        for tableName in bseAndSumTbls:
            if bseAndSumTbls[tableName].getTableType() in ('FACT', 'SUMMARY'):
                if tableName not in nonDefaultBSETables:
                    counter = 0
                    for sql in bseAndSumTbls[tableName].getSqlDropIndexes():
                        # Multiple indexes per table, but desc, below, needs
                        # to be unique
                        counter += 1
                        dfl.customSQL(
                            sql,
                            dataLayer='BSE',
                            desc='Dropping fact indexes for ' + tableName +
                                 ' (' + str(counter) + ')')
        dfl.close()

        # GET ALL DEFAULT ROWS

        # Our defaultRows SS should contain a tab per dimension, each with 1+
        # default rows defined. IDs are defined too - should all be negative
        defaultRows = {}
        worksheets = betl.CONF.DATA.getDefaultRowsDatastore().worksheets
        for wsTitle in worksheets:
            defaultRows[wsTitle] = worksheets[wsTitle].get_all_records()
        for dimOrFactLoad in loadSequence:
            for tableName in bseTables:
                tableType = bseTables[tableName].getTableType()
                if (tableType == dimOrFactLoad):
                    if tableName not in nonDefaultBSETables:
                        bulkLoadTable(betl=betl,
                                      table=bseTables[tableName],
                                      tableType=tableType,
                                      defaultRows=defaultRows)

    if betl.CONF.EXE.BULK_OR_DELTA == 'DELTA':
        for tableType in loadSequence:
            for tableName in bseTables:
                tableType = bseTables[tableName].getTableType()
                if (tableType == tableType):
                    if tableName not in nonDefaultBSETables:
                        deltaLoadTable(betl=betl,
                                       table=bseTables[tableName],
                                       tableType=tableType)


def bulkLoadTable(betl, table, tableType, defaultRows):
    if tableType == 'DIMENSION':
        bulkLoadDimension(betl=betl, defaultRows=defaultRows, table=table)
    elif tableType == 'FACT':
        bulkLoadFact(betl=betl, table=table)


def deltaLoadTable(betl, table, tableType):
    if tableType == 'DIMENSION':
        deltaLoadDimension(betl=betl, table=table)
    elif tableType == 'FACT':
        deltaLoadFact(betl=betl, table=table)


def bulkLoadDimension(betl, defaultRows, table):
    dfl = betl.DataFlow(desc='Loading dimension: ' + table.tableName)

    # DATA

    dfl.truncate(
        dataset=table.tableName,
        dataLayerID='BSE',
        forceDBWrite=True,
        desc='Because it is a bulk load, clear out the dim data (which also ' +
             'restarts the SK sequences)')

    dataset = table.tableName
    dfl.read(
        tableName=dataset,
        dataLayer='LOD',
        desc='Read the data we are going to load to BSE (from file ' +
             table.tableName + ')')

    dfl.write(
        dataset=dataset,
        targetTableName=table.tableName,
        dataLayerID='BSE',
        forceDBWrite=True,
        append_or_replace='append',  # stops it altering table & removing SK!
        writingDefaultRows=True,
        desc='Load data into the target model for ' + table.tableName,
        keepDataflowOpen=True)

    # INDEXES

    counter = 0
    for sql in table.getSqlCreateIndexes():
        counter += 1
        dfl.customSQL(
            sql,
            dataLayer='BSE',
            desc='Creating index for ' + table.tableName +
                 ' (' + str(counter) + ')')

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
            dataLayerID='BSE',
            forceDBWrite=True,
            append_or_replace='append',
            writingDefaultRows=True,
            desc='Adding default rows to ' + table.tableName,
            keepDataflowOpen=True)

    elif table.tableName == 'dm_audit' and betl.CONF.SCHEDULE.DEFAULT_DM_AUDIT:

        dfl.createDataset(
            dataset='dm_audit_default_rows',
            data={'audit_id': [-1],
                  'latest_delta_load_operation': ['N/A'],
                  'data_quality_score': [-1]},
            desc='Loading the dm_audit default row')

        dfl.write(
            dataset='dm_audit_default_rows',
            targetTableName='dm_audit',
            dataLayerID='BSE',
            forceDBWrite=True,
            append_or_replace='append',
            writingDefaultRows=True,
            desc='Adding default rows to dm_audit',
            keepDataflowOpen=True)

    # RETRIEVE SK/NK MAPPING (FOR LATER)

    skDatasetName = 'sk_' + table.tableName

    dfl.read(
        tableName=table.tableName,
        targetDataset=skDatasetName,
        dataLayer='BSE',
        forceDBRead=True,
        desc='The SKs were generated as we wrote to the DB. We will need ' +
             'these SKs (and their corresponding NKs) when we load the fact ' +
             'table (later), so we pull the sk/nks mapping back out now)')

    dfl.replace(
        dataset=skDatasetName,
        columnNames=None,
        toReplace=np.nan,
        value='',
        regex=True,
        desc='Make all None values come through as empty strings')

    dfl.dropColumns(
        dataset=skDatasetName,
        colsToKeep=[table.surrogateKeyColName] + table.colNames_NKs,
        desc='Drop all cols except SK & NKs (including audit cols)',
        dropAuditCols=True)

    dfl.renameColumns(
        dataset=skDatasetName,
        columns={table.surrogateKeyColName: 'sk'},
        desc='Rename the SK column to "sk"')

    dfl.addColumns(
        dataset=skDatasetName,
        columns={'nk': concatenateNKs},
        desc='Concatenate the NK columns into a single "nk" column')

    dfl.dropColumns(
        dataset=skDatasetName,
        colsToKeep=['sk', 'nk'],
        desc='Drop all cols except the sk col and the new nk col')

    dfl.write(
        dataset=skDatasetName,
        targetTableName=skDatasetName,
        dataLayerID='LOD')


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


def bulkLoadFact(betl, table):

    dfl = betl.DataFlow(desc='Loading fact: ' + table.tableName)

    # READ DATA

    dfl.truncate(
        dataset=table.tableName,
        dataLayerID='BSE',
        forceDBWrite=True,
        desc='Because it is a bulk load, clear out the ft data (which also ' +
             'restarts the SK sequences)')

    dfl.read(
        tableName=table.tableName,
        dataLayer='LOD',
        targetDataset=table.tableName,
        desc='Read the data we are going to load to BSE (from file ' +
             table.tableName + ')')

    # SK/NK MAPPINGS

    # collapose the dm_audit nk first

    dfl.createAuditNKs(
        dataset=table.tableName,
        desc='Collapse the audit columns into their NK')

    # now join all nks to their respective dims sk/nk mappings, & load the fact

    for column in table.columns:
        if column.isFK:
            keyMapTableName = 'sk_' + column.fkDimension
            dfl.read(
                tableName=keyMapTableName,
                targetDataset=keyMapTableName + '.' + column.columnName,
                dataLayer='LOD',
                desc='Read the SK/NK mapping for column ' + column.columnName +
                     '. Then (silently) rename the sk/nk mapping cols to ' +
                     'match the fact table, join with the dim, assign all ' +
                     'missing rows to -1 row, & drop the nk col from the fact')

            nkColName = column.columnName.replace('fk_', 'nk_')

            dfl.renameColumns(
                dataset=keyMapTableName + '.' + column.columnName,
                columns={
                    'sk': column.columnName,
                    'nk': nkColName},
                desc='Rename the columns of the ' + column.fkDimension + ' ' +
                     'SK/NK mapping to match the fact table column names ' +
                     ' (' + column.columnName + ')',
                silent=True)

            dfl.join(
                datasets=[
                    table.tableName,
                    keyMapTableName + '.' + column.columnName],
                targetDataset=table.tableName,
                joinCol=nkColName,
                how='left',
                desc="Merging dim's SK with fact for column " +
                     column.columnName,
                silent=True)

            dfl.setNulls(
                dataset=table.tableName,
                columns={column.columnName: -1},
                desc='Assigning all missing rows to default -1 row (' +
                     column.columnName + ')',
                silent=True)

            dfl.dropColumns(
                dataset=table.tableName,
                colsToDrop=[nkColName],
                desc='Dropping the natural key column: ' + nkColName,
                silent=True)

    # WRITE DATA

    dfl.write(
        dataset=table.tableName,
        targetTableName=table.tableName,
        dataLayerID='BSE',
        append_or_replace='append',  # stops it altering table & removing SK!
        keepDataflowOpen=True)

    # INDEXES

    counter = 0
    for sql in table.getSqlCreateIndexes():
        counter += 1
        dfl.customSQL(
            sql,
            dataLayer='BSE',
            desc='Creating index for ' + table.tableName +
                 ' (' + str(counter) + ')')
    dfl.close()


def deltaLoadDimension(betl, table):
    raise ValueError("Code not yet written for delta dimension loads")


def deltaLoadFact(betl, table):
    raise ValueError("Code not yet written for delta fact loads")
