from . import api as betl

import pandas as pd
from datetime import datetime


#
# A default extraction process. Bulk is obvious and as you would expect
# Delta does full-table comparisons to identify deltas
#
def defaultExtract(scheduler):
    if scheduler.bulkOrDelta == 'BULK':
        defaultExtract_bulk(scheduler)
    elif scheduler.bulkOrDelta == 'DELTA':
        defaultExtract_delta(scheduler)


def defaultExtract_bulk(scheduler):

    srcTablesToExclude = scheduler.srcTablesToExcludeFromExtract
    srcLayer = scheduler.logicalDataModels['SRC']

    for dmID in srcLayer.dataModels:
        for tableName in srcLayer.dataModels[dmID].tables:
            if tableName in srcTablesToExclude:
                continue

            dfl = betl.DataFlow(desc='Default extract for ' + tableName)

            dfl.getDataFromSrc(
                tableName=tableName,
                srcSysID=dmID)

            dfl.setAuditCols(
                dataset=tableName,
                bulkOrDelta="BULK",
                sourceSystem=dmID)

            dfl.write(
                dataset=tableName,
                targetTableName=tableName,
                dataLayerID='SRC')


def defaultExtract_delta(scheduler):

    # TODO not been refactored since dataframe class added to betl

    srcTablesToExclude = scheduler.srcTablesToExcludeFromExtract
    srcLayer = scheduler.logicalDataModels['SRC']

    for dmID in srcLayer.dataModels:
        for tableName in srcLayer.dataModels[dmID].tables:
            if tableName in srcTablesToExclude:
                continue
    colNameList = \
        srcLayer.dataModels[dmID].tables[tableName].colNames
    nkList = \
        srcLayer.dataModels[dmID].tables[tableName].colNames_NKs
    nonNkList = \
        srcLayer.dataModels[dmID].tables[tableName].colNames_withoutNKs

    if len(nkList) == 0:
        raise ValueError(tableName + ' does not have a natural ' +
                         'key defined, so we cannot run a delta ' +
                         'load. Aborting.')

    # We identify the deltas by comparing the source system table
    # to our SRC layer in the ETL database, using
    # merge(). After each merge we get _src and _stg columns.

    # NOTE: wrong language here in these suffixes.
    # We need to be able to easily strip back to the columns we
    # want, which depends whether we're keeping left_only (inserts)
    # or right_only (deletes)

    insertcolNameList = []
    updatecolNameList = []
    deletecolNameList = []

    columns = srcLayer.dataModels[dmID]     \
        .tables[tableName].columns

    for column in columns:
        if column.isNK:
            insertcolNameList.append(column.columnName)
            updatecolNameList.append(column.columnName)
            deletecolNameList.append(column.columnName)
        elif column.columnName.find('audit_') == 0:
            insertcolNameList.append(column.columnName)
            deletecolNameList.append(column.columnName)
        else:
            insertcolNameList.append(column.columnName + '_src')
            updatecolNameList.append(column.columnName)
            deletecolNameList.append(column.columnName + '_stg')

    stgDF = betl.readData(tableName, 'SRC')

    deltaDF = pd.merge(left=srcDF, right=stgDF, how='outer',
                       suffixes=('_src', '_stg'), on=nkList,
                       indicator=True)

    ###########
    # INSERTS #
    ###########

    # Pull out the inserts and tidy
    insertsDF = deltaDF.loc[deltaDF['_merge'] == 'left_only',
                            insertcolNameList]
    insertsDF.columns = colNameList

    # Apply inserts, to DB and DF
    if not insertsDF.empty:
        insertsDF =                                               \
            scheduler.dataIO.setAuditCols(df=insertsDF,
                                          sourceSystemId=dmID,
                                          action='INSERT')
        # NOTE: Check this logic
        betl.writeData(insertsDF, tableName, 'SRC', 'append')
        stgDF = stgDF.append(insertsDF, ignore_index=True,
                             verify_integrity=True)
    else:
        pass

    ###########
    # DELETES #
    ###########

    # Pull out the deletes and tidy
    deletesDF = deltaDF.loc[deltaDF['_merge'] == 'right_only',
                            deletecolNameList]
    deletesDF.columns = colNameList
    # to do #10

    # Apply deletes, to DB and DF
    if not deletesDF.empty:
        deletesDF = scheduler.dataIO.setAuditCols(
            df=deletesDF,
            sourceSystemId=dmID,
            action='DELETE')
        etlDbCursor = scheduler.dataIO.ETL_DB_CONN.cursor()
        for index, row in deletesDF.iterrows():

            nkWhereClause = 'WHERE'
            index = 0
            for columnName in nkList:
                if index > 0:
                    nkWhereClause += " AND " + columnName + " = '"\
                                     + row[columnName] + "'"
                else:
                    nkWhereClause += " " + columnName + " = '" \
                                     + row[columnName] + "'"
                index += 1

            etlDbCursor.execute("DELETE FROM src_ipa_addresses "
                                + nkWhereClause)
        scheduler.dataIO.ETL_DB_CONN.commit()
        stgDF = pd.concat([stgDF, deletesDF])                 \
            .drop_duplicates(keep=False)
    else:
        pass

    ###########
    # UPDATES #
    ###########

    # Merge will look for differences between all columns, so we
    # need to trip out the audit columns (which we know will
    # differ, but shouldn't trigger an update. Obvs.)
    # This works because we've already applied inserts and deletes
    # to the dfs (they would show up again, otherwise)

    srcDF = srcDF[colNameList]
    stgDF = stgDF[colNameList]

    # Compare the two dataframes again, this time across all rows,
    # to pick up edits
    deltaDF = pd.merge(left=srcDF,
                       right=stgDF,
                       how='outer',
                       suffixes=('_src', '_stg'),
                       indicator=True)

    # Pull out the updates and tidy
    updatesDF = deltaDF.loc[deltaDF['_merge'] == 'left_only',
                            updatecolNameList]
    updatesDF.columns = colNameList

    # Apply updates, to DB and DF
    if not updatesDF.empty:
        updatesDF = betl.setAuditCols(df=updatesDF,
                                      sourceSystemId=dmID,
                                      action='UPDATE')
        etlDbCursor = scheduler.conf.ETL_DB_CONN.cursor()
        for index, row in updatesDF.iterrows():

            nkWhereClause = 'WHERE'
            index = 0
            for columnName in nkList:
                if index > 0:
                    nkWhereClause += " AND " + columnName + " = '"\
                                     + str(row[columnName]) + "'"
                else:
                    nkWhereClause += " " + columnName + " = '"    \
                                     + str(row[columnName]) + "'"
                index += 1

            nonNkSetClause = 'SET'
            index = 0
            for columnName in nonNkList:
                if columnName == 'audit_bulk_load_date':
                    pass
                else:
                    if index > 0:
                        nonNkSetClause += ", " + columnName       \
                                          + " = '"                \
                                          + str(row[columnName])  \
                                          + "'"
                    else:
                        nonNkSetClause += " " + columnName        \
                                          + " = '"                \
                                          + str(row[columnName])  \
                                          + "'"
                    index += 1
            etlDbCursor.execute("UPDATE src_ipa_addresses "
                                + nonNkSetClause + " "
                                + nkWhereClause)
        scheduler.conf.data.getDatastore('ETL').commit()
    else:
        pass


#
# Functions to set the audit columns on the dataframes, prior to loading
# into persistent storage
#

def setAuditCols_insert(self, df, sourceSystemId):

    df['audit_source_system'] = sourceSystemId
    df['audit_bulk_load_date'] = None
    df['audit_latest_delta_load_date'] = datetime.now()
    df['audit_latest_load_operation'] = 'INSERT'

    return df


def setAuditCols_update(self, df, sourceSystemId):

    df['audit_source_system'] = sourceSystemId
    df['audit_latest_delta_load_date'] = datetime.now()
    df['audit_latest_load_operation'] = 'UPDATE'

    return df


def setAuditCols_delete(self, df):

    df['audit_latest_delta_load_date'] = datetime.now()
    df['audit_latest_load_operation'] = 'DELETE'

    return df
