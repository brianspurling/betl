from . import logger as logger
import pandas as pd
from . import api


#
# A default extraction process. Bulk is obvious and as you would expect
# Delta does full-table comparisons to identify deltas
#
def defaultExtract(scheduler):

    devLog = logger.getDevLog(__name__)

    # to do #9
    srcTablesToExclude = scheduler.srcTablesToExcludeFromExtract
    srcLayer = scheduler.logicalDataModels['SRC']

    devLog.info("START")

    for dmID in srcLayer.dataModels:
        for tableName in srcLayer.dataModels[dmID].tables:
            # The app might want to take care of some tables itself, rather
            # than using the default. These will have been passed in, so if
            # this table is one of them let's skip
            if tableName in srcTablesToExclude:
                devLog.info("Skipping default extract for " + tableName)
                continue

            colNameList = \
                srcLayer.dataModels[dmID].tables[tableName].colNames
            nkList = \
                srcLayer.dataModels[dmID].tables[tableName].colNames_NKs
            nonNkList = \
                srcLayer.dataModels[dmID].tables[tableName].colNames_withoutNKs
            srcDF = scheduler.dataIO.readDataFromSrcSys(
                srcSysID=dmID,
                file_name_or_table_name=tableName)

            if scheduler.bulkOrDelta == 'BULK':

                srcDF =                                                       \
                    api.setAuditCols(df=srcDF,
                                     srcSysID=dmID,
                                     action='BULK')

                # Bulk write the SRC table
                api.writeData(srcDF, tableName, 'SRC')

            elif scheduler.bulkOrDelta == 'DELTA':

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

                stgDF = api.readData(tableName, 'SRC')

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
                    api.writeData(insertsDF, tableName, 'SRC', 'append')
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
                    updatesDF = api.setAuditCols(df=updatesDF,
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
                    scheduler.conf.app.DWH_DATABASES['ETL'].commit()
                else:
                    pass
