from . import utilities as utils
from . import schemas
from . import conf
import pprint
import pandas as pd
from datetime import datetime

log = utils.setUpLogger('EXTRCT', __name__)


#
# A default extraction process. Bulk is obvious and as you would expect
# Delta does full-table comparisons to identify deltas
#
def defaultExtract():

    # to do #9
    logStr = ''
    srcTablesToExclude = conf.SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT

    log.debug("START")

    for dataModelId in schemas.SRC_LAYER.dataModels:
        for tableName in schemas.SRC_LAYER.dataModels[dataModelId].tables:
            # The app might want to take care of some tables itself, rather
            # than using the default. These will have been passed in, so if
            # this table is one of them let's skip
            if tableName in srcTablesToExclude:
                log.info("Skipping default extract for " + tableName)
                continue

            tableShortName = schemas.SRC_LAYER.dataModels[dataModelId]        \
                .tables[tableName].tableShortName

            colNameList = schemas.SRC_LAYER.dataModels[dataModelId]           \
                .tables[tableName].colNameList
            colNameList = schemas.SRC_LAYER                                   \
                .dataModels[dataModelId]                                      \
                .tables[tableName]                                            \
                .colNameList
            nkList = schemas.SRC_LAYER.dataModels[dataModelId]                \
                .tables[tableName].nkList
            nonNkList = schemas.SRC_LAYER.dataModels[dataModelId]             \
                .tables[tableName].nonNkList

            # This is what we're going to read our data into
            srcDF = pd.DataFrame()

            srcSysType = schemas.SRC_LAYER.srcSystemConns[dataModelId].type
            if srcSysType == 'POSTGRES':
                srcConn = schemas.SRC_LAYER.srcSystemConns[dataModelId].conn
                srcDF = utils.readFromSrcDB(tableShortName, srcConn)

            elif srcSysType == 'FILESYSTEM':
                fullTableName = tableShortName + '.csv',
                srcDF = pd.read_csv(filepath_or_buffer=fullTableName,
                                    sep=schemas.SRC_LAYER
                                    .srcSystemConns[dataModelId]
                                    .files[tableShortName]['delimiter'],
                                    quotechar=schemas.SRC_LAYER
                                    .srcSystemConns[dataModelId]
                                    .files[tableShortName]['quotechar'])

            elif srcSysType == 'SPREADSHEET':
                data = schemas.SRC_LAYER                                      \
                    .srcSystemConns[dataModelId]                              \
                    .worksheets[tableName].get_all_values()
                srcDF = pd.DataFrame(data[3:], columns=data[0])

            else:
                raise ValueError('Extract for source systems type <'
                                 + srcSysType
                                 + '> connection type not supported')

            log.info('Extracted ' + str(srcDF.shape)
                     + ' from source system ' + dataModelId
                     + ', table = ' + tableShortName)

            if conf.BULK_OR_DELTA == 'BULK':

                srcDF =                                                       \
                    utils.setAuditCols(df=srcDF,
                                       sourceSystemId=dataModelId,
                                       action='BULK')

                # Bulk write the SRC table
                time = str(datetime.time(datetime.now()))
                log.info('bulk writing ' + tableName
                         + ' to SRC (start: ' + time + ')')

                # if_exists='replace' covers the truncate for us
                srcDF.to_sql(tableName,
                             conf.ETL_DB_ENG,
                             if_exists='replace',
                             index=False)

                time = str(datetime.time(datetime.now()))
                log.info(tableName
                         + ' written to SRC (end: ' + time + ')')

            elif conf.BULK_OR_DELTA == 'DELTA':

                if len(nkList) == 0:
                    raise ValueError(tableName + ' does not have a natural ' +
                                     'key defined, so we cannot run a delta ' +
                                     'load. Aborting.')

                # We identify the deltas by comparing the source system table
                # to our SRC layer in the ETL database, using
                # merge(). After each merge we get _src and _stg columns. TODO
                # wrong language here in these suffixes.
                # We need to be able to easily strip back to the columns we
                # want, which depends whether we're keeping left_only (inserts)
                # or right_only (deletes)

                insertcolNameList = []
                updatecolNameList = []
                deletecolNameList = []

                columns = schemas.SRC_LAYER.dataModels[dataModelId]     \
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

                stgDF = utils.readFromEtlDB(tableName)

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
                    time = str(datetime.time(datetime.now()))
                    log.info('Applying ' + str(insertsDF.shape)
                             + ' inserts to ' + tableName
                             + ' (start: ' + time + ')')
                    insertsDF =                                               \
                        utils.setAuditCols(df=insertsDF,
                                           sourceSystemId=dataModelId,
                                           action='INSERT')
                    insertsDF.to_sql(tableName, conf.ETL_DB_ENG,
                                     if_exists='append',
                                     index=False)
                    stgDF = stgDF.append(insertsDF, ignore_index=True,
                                         verify_integrity=True)
                    time = str(datetime.time(datetime.now()))
                    log.info('Inserts applied ' + str(insertsDF.shape)
                             + ' inserts to ' + tableName
                             + ' (end: ' + time + ')')
                else:
                    log.info('No inserts found for ' + tableName)

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
                    time = str(datetime.time(datetime.now()))
                    log.info('Applying ' + str(deletesDF.shape) + ' deletes to'
                             + ' ' + tableName + ' (start: ' + time + ')')
                    deletesDF = utils.setAuditCols(df=deletesDF,
                                                   sourceSystemId=dataModelId,
                                                   action='DELETE')
                    etlDbCursor = conf.ETL_DB_CONN.cursor()
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
                    conf.ETL_DB_CONN.commit()
                    time = str(datetime.time(datetime.now()))
                    log.info('Deletes applied (end: ' + time + ')')
                    stgDF = pd.concat([stgDF, deletesDF])                 \
                        .drop_duplicates(keep=False)
                else:
                    log.info('No deletes found for ' + tableName)

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
                    time = str(datetime.time(datetime.now()))
                    log.info('Applying ' + str(updatesDF.shape) + ' updates to'
                             + ' ' + tableName + ' (start: ' + time + ')')
                    updatesDF = utils.setAuditCols(df=updatesDF,
                                                   sourceSystemId=dataModelId,
                                                   action='UPDATE')
                    etlDbCursor = conf.ETL_DB_CONN.cursor()
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
                    conf.ETL_DB_CONN.commit()
                    time = str(datetime.time(datetime.now()))
                    log.info('Updates applied (end: ' + time + ')')
                else:
                    log.info('No updates found for ' + tableName)

    return logStr
