def logExtractStart(**kwargs):
    kwargs['conf'].log('logExtractStart')


def logExtractEnd(**kwargs):
    kwargs['conf'].log('logExtractEnd')


def logSkipExtract(**kwargs):
    kwargs['conf'].log('logSkipExtract')


def bulkExtract(**kwargs):

    conf = kwargs['conf']
    tableName = kwargs['tableName']
    dmId = kwargs['dmId']

    dfl = conf.DataFlow(desc='Default extract for ' + tableName)

    extLayer = conf.getLogicalSchemaDataLayer('EXT')

    dfl.getDataFromSrc(
        tableName=tableName,
        srcSysID=dmId,
        desc="Extract data from source table",
        srcTableName=extLayer.datasets[dmId].tables[tableName].srcTableName)

    dfl.setAuditCols(
        dataset=tableName,
        bulkOrDelta="BULK",
        sourceSystem=dmId,
        desc="Set the audit columns on the data extract")

    dfl.write(
        dataset=tableName,
        targetTableName=tableName,
        dataLayerID='EXT',
        desc="Write the data extract to the SRC data layer")

# def defaultExtract_delta(**kwargs):

    # TODO not been refactored since dataframe class added to betl

    # srcTablesToExclude = \
    #     scheduler.CONF.EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT
    #
    # extLayer = scheduler.CONF.getLogicalSchemaDataLayer('EXT')
    #
    # for dmId in extLayer.datasets:
    #     for tableName in extLayer.datasets[dmId].tables:
    #         if tableName in srcTablesToExclude:
    #             continue
    # colNameList = \
    #     extLayer.datasets[dmId].tables[tableName].colNames
    # nkList = \
    #     extLayer.datasets[dmId].tables[tableName].colNames_NKs
    # nonNkList = \
    #     extLayer.datasets[dmId].tables[tableName].colNames_withoutNKs
    #
    # if len(nkList) == 0:
    #     raise ValueError(tableName + ' does not have a natural ' +
    #                      'key defined, so we cannot run a delta ' +
    #                      'load. Aborting.')
    #
    # # We identify the deltas by comparing the source system table
    # # to our SRC layer in the ETL database, using
    # # merge(). After each merge we get _src and _stg columns.
    #
    # # NOTE: wrong language here in these suffixes.
    # # We need to be able to easily strip back to the columns we
    # # want, which depends whether we're keeping left_only (inserts)
    # # or right_only (deletes)
    #
    # insertcolNameList = []
    # updatecolNameList = []
    # deletecolNameList = []
    #
    # columns = extLayer.datasets[dmId]     \
    #     .tables[tableName].columns
    #
    # for column in columns:
    #     if column.isNK:
    #         insertcolNameList.append(column.columnName)
    #         updatecolNameList.append(column.columnName)
    #         deletecolNameList.append(column.columnName)
    #     elif column.columnName.find('audit_') == 0:
    #         insertcolNameList.append(column.columnName)
    #         deletecolNameList.append(column.columnName)
    #     else:
    #         insertcolNameList.append(column.columnName + '_src')
    #         updatecolNameList.append(column.columnName)
    #         deletecolNameList.append(column.columnName + '_stg')
    #
    # stgDF = conf.readData(tableName, 'EXT')
    #
    # deltaDF = pd.merge(left=srcDF, right=stgDF, how='outer',
    #                    suffixes=('_src', '_stg'), on=nkList,
    #                    indicator=True)
    #
    # ###########
    # # INSERTS #
    # ###########
    #
    # # Pull out the inserts and tidy
    # insertsDF = deltaDF.loc[deltaDF['_merge'] == 'left_only',
    #                         insertcolNameList]
    # insertsDF.columns = colNameList
    #
    # # Apply inserts, to DB and DF
    # if not insertsDF.empty:
    #     insertsDF =                                               \
    #         scheduler.dataIO.setAuditCols(df=insertsDF,
    #                                       sourceSystemId=dmId,
    #                                       action='INSERT')
    #     # NOTE: Check this logic
    #     conf.writeData(insertsDF, tableName, 'EXT', 'append')
    #     stgDF = stgDF.append(insertsDF, ignore_index=True,
    #                          verify_integrity=True)
    # else:
    #     pass
    #
    # ###########
    # # DELETES #
    # ###########
    #
    # # Pull out the deletes and tidy
    # deletesDF = deltaDF.loc[deltaDF['_merge'] == 'right_only',
    #                         deletecolNameList]
    # deletesDF.columns = colNameList
    # # to do #10
    #
    # # Apply deletes, to DB and DF
    # if not deletesDF.empty:
    #     deletesDF = scheduler.dataIO.setAuditCols(
    #         df=deletesDF,
    #         sourceSystemId=dmId,
    #         action='DELETE')
    #     etlDbCursor = scheduler.dataIO.ETL_DB_CONN.cursor()
    #     for index, row in deletesDF.iterrows():
    #
    #         nkWhereClause = 'WHERE'
    #         index = 0
    #         for columnName in nkList:
    #             if index > 0:
    #                 nkWhereClause += " AND " + columnName + " = '"\
    #                                  + row[columnName] + "'"
    #             else:
    #                 nkWhereClause += " " + columnName + " = '" \
    #                                  + row[columnName] + "'"
    #             index += 1
    #
    #         etlDbCursor.execute("DELETE FROM src_ipa_addresses "
    #                             + nkWhereClause)
    #     scheduler.dataIO.ETL_DB_CONN.commit()
    #     stgDF = pd.concat([stgDF, deletesDF])                 \
    #         .drop_duplicates(keep=False)
    # else:
    #     pass
    #
    # ###########
    # # UPDATES #
    # ###########
    #
    # # Merge will look for differences between all columns, so we
    # # need to trip out the audit columns (which we know will
    # # differ, but shouldn't trigger an update. Obvs.)
    # # This works because we've already applied inserts and deletes
    # # to the dfs (they would show up again, otherwise)
    #
    # srcDF = srcDF[colNameList]
    # stgDF = stgDF[colNameList]
    #
    # # Compare the two dataframes again, this time across all rows,
    # # to pick up edits
    # deltaDF = pd.merge(left=srcDF,
    #                    right=stgDF,
    #                    how='outer',
    #                    suffixes=('_src', '_stg'),
    #                    indicator=True)
    #
    # # Pull out the updates and tidy
    # updatesDF = deltaDF.loc[deltaDF['_merge'] == 'left_only',
    #                         updatecolNameList]
    # updatesDF.columns = colNameList
    #
    # # Apply updates, to DB and DF
    # if not updatesDF.empty:
    #     updatesDF = conf.setAuditCols(df=updatesDF,
    #                                   sourceSystemId=dmId,
    #                                   action='UPDATE')
    #     etlDbCursor = scheduler.conf.ETL_DB_CONN.cursor()
    #     for index, row in updatesDF.iterrows():
    #
    #         nkWhereClause = 'WHERE'
    #         index = 0
    #         for columnName in nkList:
    #             if index > 0:
    #                 nkWhereClause += " AND " + columnName + " = '"\
    #                                  + str(row[columnName]) + "'"
    #             else:
    #                 nkWhereClause += " " + columnName + " = '"    \
    #                                  + str(row[columnName]) + "'"
    #             index += 1
    #
    #         nonNkSetClause = 'SET'
    #         index = 0
    #         for columnName in nonNkList:
    #             if columnName == 'audit_bulk_load_date':
    #                 pass
    #             else:
    #                 if index > 0:
    #                     nonNkSetClause += ", " + columnName       \
    #                                       + " = '"                \
    #                                       + str(row[columnName])  \
    #                                       + "'"
    #                 else:
    #                     nonNkSetClause += " " + columnName        \
    #                                       + " = '"                \
    #                                       + str(row[columnName])  \
    #                                       + "'"
    #                 index += 1
    #         etlDbCursor.execute("UPDATE src_ipa_addresses "
    #                             + nonNkSetClause + " "
    #                             + nkWhereClause)
    #     scheduler.CONF.getDWHDatastore('ETL').commit()
    # else:
    #     pass


#
# Functions to set the audit columns on the dataframes, prior to loading
# into persistent storage
#
#
# def setAuditCols_insert(self, df, sourceSystemId):
#
#     df['audit_source_system'] = sourceSystemId
#     df['audit_bulk_load_date'] = None
#     df['audit_latest_delta_load_date'] = datetime.now()
#     df['audit_latest_load_operation'] = 'INSERT'
#
#     return df
#
#
# def setAuditCols_update(self, df, sourceSystemId):
#
#     df['audit_source_system'] = sourceSystemId
#     df['audit_latest_delta_load_date'] = datetime.now()
#     df['audit_latest_load_operation'] = 'UPDATE'
#
#     return df
#
#
# def setAuditCols_delete(self, df):
#
#     df['audit_latest_delta_load_date'] = datetime.now()
#     df['audit_latest_load_operation'] = 'DELETE'
#
#     return df
