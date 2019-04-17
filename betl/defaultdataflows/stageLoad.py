import pandas as pd
import numpy as np
import json
import os
import ast


def logLoadStart(**kwargs):
    kwargs['conf'].log('logLoadStart')


def logBulkLoadSetupStart(**kwargs):
    kwargs['conf'].log('logBulkLoadSetupStart')


def logBulkLoadSetupEnd(**kwargs):
    kwargs['conf'].log('logBulkLoadSetupEnd')


def logDimLoadStart(**kwargs):
    kwargs['conf'].log('logDimLoadStart')


def logDefaultDimLoadStart(**kwargs):
    kwargs['conf'].log('logDefaultDimLoadStart')


def logDefaultDimLoadEnd(**kwargs):
    kwargs['conf'].log('logDefaultDimLoadEnd')


def logBespokeDimLoadStart(**kwargs):
    kwargs['conf'].log('logBespokeDimLoadStart')


def logBespokeDimLoadEnd(**kwargs):
    kwargs['conf'].log('logBespokeDimLoadEnd')


def logDimLoadEnd(**kwargs):
    kwargs['conf'].log('logDimLoadEnd')


def logFactLoadStart(**kwargs):
    kwargs['conf'].log('logFactLoadStart')


def logDefaultFactLoadStart(**kwargs):
    kwargs['conf'].log('logDefaultFactLoadStart')


def logDefaultFactLoadEnd(**kwargs):
    kwargs['conf'].log('logDefaultFactLoadEnd')


def logBespokeFactLoadStart(**kwargs):
    kwargs['conf'].log('logBespokeFactLoadStart')


def logBespokeFactLoadEnd(**kwargs):
    kwargs['conf'].log('logBespokeFactLoadEnd')


def logFactLoadEnd(**kwargs):
    kwargs['conf'].log('logFactLoadEnd')


def logLoadEnd(**kwargs):
    kwargs['conf'].log('logLoadEnd')


def logSkipLoad(**kwargs):
    kwargs['conf'].log('logSkipLoad')


def refreshDefaultRowsTxtFileFromGSheet(**kwargs):

    conf = kwargs['conf']

    conf.log('logRefreshDefaultRowsTxtFileFromGSheetStart')

    # Our defaultRows SS should contain a tab per dimension, each with 1+
    # default rows defined. IDs are defined too - should all be negative
    defaultRowsDatastore = conf.getDefaultRowsDatastore()
    worksheets = {}
    if defaultRowsDatastore is not None:
        worksheets = defaultRowsDatastore.worksheets
    for wsTitle in worksheets:
        # wsTitle is the table name
        filename = conf.TMP_DATA_PATH + '/defaultRows_' + wsTitle + '.txt'
        with open(filename, 'w') as file:
            file.write(json.dumps(worksheets[wsTitle].get_all_records()))
    conf.log('logRefreshDefaultRowsTxtFileFromGSheetEnd')


def dropFactFKConstraints(**kwargs):

    conf = kwargs['conf']

    bseLayer = conf.getLogicalSchemaDataLayer('BSE')
    sumLayer = conf.getLogicalSchemaDataLayer('SUM')

    bseTables = bseLayer.datasets['BSE'].tables
    sumTables = {}
    if 'SUM' in sumLayer.datasets:
        sumTables = sumLayer.datasets['SUM'].tables

    bseAndSumTbls = {**bseTables, **sumTables}

    dfl = conf.DataFlow(
        desc="If it's a bulk load, drop the indexes to speed up " +
             "writing. We do this here, because we need to drop " +
             "fact indexes first (or, to be precise, the facts' " +
             "foreign key constraints, because the dim ID indexes " +
             " cant be dropped until the FKs that point to them are gone)")

    for tableName in bseAndSumTbls:
        if bseAndSumTbls[tableName].getTableType() in ('FACT', 'SUMMARY'):
            skip = conf.BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD
            if tableName not in skip:
                counter = 0
                for sql in bseAndSumTbls[tableName].getSqlDropIndexes():
                    # Multiple indexes per table, but desc, below, needs
                    # to be unique
                    counter += 1
                    dfl.customSQL(
                        sql,
                        databaseID='TRG',
                        desc='Dropping fact indexes for ' + tableName +
                             ' (' + str(counter) + ')')
    dfl.close()


def bulkLoad(**kwargs):
    conf = kwargs['conf']
    tableName = kwargs['tableName']
    tableSchema = kwargs['tableSchema']
    tableType = kwargs['tableType']

    if tableType == 'DIMENSION':

        # Dimension load includes any manually-created default_rows

        filePath = conf.TMP_DATA_PATH + '/defaultRows_' + tableName + '.txt'

        defaultRows = {}
        if os.path.exists(filePath):
            defaultRowsFile = open(filePath, 'r')
            fileContent = defaultRowsFile.read()
            defaultRows = ast.literal_eval(fileContent)

        bulkLoadDimension(conf=conf,
                          tableSchema=tableSchema,
                          defaultRows=defaultRows)

    elif tableType == 'FACT':

        bulkLoadFact(
            conf=conf,
            tableSchema=tableSchema)


def deltaLoad(**kwargs):
    conf = kwargs['conf']
    tableSchema = kwargs['tableSchema']
    tableType = kwargs['tableType']

    if tableType == 'DIMENSION':

        deltaLoadDimension(conf=conf,
                           tableSchema=tableSchema)

    elif tableType == 'FACT':

        deltaLoadFact(
            conf=conf,
            tableSchema=tableSchema)


#
# Remaining functions are not accessed directly by Airflow
#

def bulkLoadDimension(conf, tableSchema, defaultRows):

    dfl = conf.DataFlow(desc='Loading dimension: ' + tableSchema.tableName)

    # DATA

    dfl.truncate(
        dataset=tableSchema.tableName,
        dataLayerID='BSE',
        forceDBWrite=True,
        desc='Because it is a bulk load, clear out the dim data (which also ' +
             'restarts the SK sequences)')

    dfl.read(
        tableName=tableSchema.tableName,
        dataLayer='LOD',
        desc='Read the data we are going to load to BSE (from file ' +
             tableSchema.tableName + ')')

    dfl.write(
        dataset=tableSchema.tableName,
        targetTableName=tableSchema.tableName,
        dataLayerID='BSE',
        forceDBWrite=True,
        append_or_replace='append',  # stops it altering table & removing SK!
        writingDefaultRows=True,
        desc='Load data into the target model for ' + tableSchema.tableName,
        keepDataflowOpen=True)

    # INDEXES

    counter = 0
    for sql in tableSchema.getSqlCreateIndexes():
        counter += 1
        dfl.customSQL(
            sql,
            databaseID='TRG',
            desc='Creating index for ' + tableSchema.tableName +
                 ' (' + str(counter) + ')')

    # DEFAULT ROWS

    # Default rows are not compulsory, so defaultRows may be empty dict
    if defaultRows:

        # Blank values in the spreadsheet come through as empty strings, but
        # should be NULL in the DB
        # TODO: this should be a dataflow op, part of effort to tidy up the
        # apply functions
        df = pd.DataFrame.from_dict(defaultRows)
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        dfl.createDataset(
            dataset=tableSchema.tableName + '_defaultRows',
            data=df,
            desc='Loading the default rows into the dataflow')

        dfl.write(
            dataset=tableSchema.tableName + '_defaultRows',
            targetTableName=tableSchema.tableName,
            dataLayerID='BSE',
            forceDBWrite=True,
            append_or_replace='append',
            writingDefaultRows=True,
            desc='Adding default rows to ' + tableSchema.tableName,
            keepDataflowOpen=True)

    elif tableSchema.tableName == 'dm_audit' and conf.DEFAULT_DM_AUDIT:

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

    skDatasetName = 'sk_' + tableSchema.tableName

    dfl.read(
        tableName=tableSchema.tableName,
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

    colsToKeep = [tableSchema.surrogateKeyColName] + tableSchema.colNames_NKs
    dfl.dropColumns(
        dataset=skDatasetName,
        colsToKeep=colsToKeep,
        desc='Drop all cols except SK & NKs (including audit cols)',
        dropAuditCols=True)

    dfl.renameColumns(
        dataset=skDatasetName,
        columns={tableSchema.surrogateKeyColName: 'sk'},
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


def bulkLoadFact(conf, tableSchema):

    dfl = conf.DataFlow(desc='Loading fact: ' + tableSchema.tableName)

    # READ DATA

    dfl.truncate(
        dataset=tableSchema.tableName,
        dataLayerID='BSE',
        forceDBWrite=True,
        desc='Because it is a bulk load, clear out the ft data (which also ' +
             'restarts the SK sequences)')

    dfl.read(
        tableName=tableSchema.tableName,
        dataLayer='LOD',
        targetDataset=tableSchema.tableName,
        desc='Read the data we are going to load to BSE (from file ' +
             tableSchema.tableName + ')')

    # SK/NK MAPPINGS

    # collapose the dm_audit nk first

    dfl.createAuditNKs(
        dataset=tableSchema.tableName,
        desc='Collapse the audit columns into their NK')

    # now join all nks to their respective dims sk/nk mappings, & load the fact

    for column in tableSchema.columns:
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
                    tableSchema.tableName,
                    keyMapTableName + '.' + column.columnName],
                targetDataset=tableSchema.tableName,
                joinCol=nkColName,
                how='left',
                desc="Merging dim's SK with fact for column " +
                     column.columnName,
                silent=True)

            dfl.setNulls(
                dataset=tableSchema.tableName,
                columns={column.columnName: -1},
                desc='Assigning all missing rows to default -1 row (' +
                     column.columnName + ')',
                silent=True)

            dfl.dropColumns(
                dataset=tableSchema.tableName,
                colsToDrop=[nkColName],
                desc='Dropping the natural key column: ' + nkColName,
                silent=True)

    # WRITE DATA

    dfl.write(
        dataset=tableSchema.tableName,
        targetTableName=tableSchema.tableName,
        dataLayerID='BSE',
        append_or_replace='append',  # stops it altering table & removing SK!
        keepDataflowOpen=True)

    # INDEXES

    counter = 0
    for sql in tableSchema.getSqlCreateIndexes():
        counter += 1
        dfl.customSQL(
            sql,
            databaseID='TRG',
            desc='Creating index for ' + tableSchema.tableName +
                 ' (' + str(counter) + ')')
    dfl.close()


def deltaLoadDimension(conf, tableSchema):
    raise ValueError("Code not yet written for delta dimension loads")


def deltaLoadFact(conf, tableSchema):
    raise ValueError("Code not yet written for delta fact loads")
