def defaultTransform(betl):

    # TODO: make a default df_transform, that runs this on all fact tables
    # at the end of the transform stage, before load starts, and as part of
    # its own df. Otherwise if the (long and complex) load falls over, this
    # step breaks on rerun

    trgTables = \
        betl.CONF.DATA.getLogicalDataModel('TRG').dataModels['TRG'].tables

    nonDefaultStagingTables = \
        betl.CONF.SCHEDULE.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    for tableName in trgTables:
        if (trgTables[tableName].getTableType() == 'FACT'):
            if tableName not in nonDefaultStagingTables:
                dfl = betl.DataFlow(
                    desc='Default transform: convert the audit columns into ' +
                         'NKs for ' + tableName)

                dfl.read(
                    tableName='trg_' + tableName,
                    dataLayer='STG')

                dfl.createAuditNKs(
                    dataset='trg_' + tableName,
                    desc='Collapse the audit columns into their NK')

                dfl.write(
                    dataset='trg_' + tableName,
                    targetTableName='trg_' + tableName,
                    dataLayerID='STG')
