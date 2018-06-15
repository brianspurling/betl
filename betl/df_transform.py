from . import main


def defaultTransform(scheduler):

    # TODO: make a default df_transform, that runs this on all fact tables
    # at the end of the transform stage, before load starts, and as part of
    # its own df. Otherwise if the (long and complex) load falls over, this
    # step breaks on rerun

    trgTables = \
        scheduler.conf.data.getLogicalDataModel('TRG').dataModels['TRG'].tables

    nonDefaultStagingTables = \
        scheduler.conf.schedule.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    for tableName in trgTables:
        if (trgTables[tableName].getTableType() == 'FACT'):
            if tableName not in nonDefaultStagingTables:
                dfl = main.DataFlow(
                    desc='Default transform: convert the audit columns into ' +
                         'NKs for ' + tableName)

                dfl.read(tableName='trg_' + tableName,
                         dataLayer='STG')

                dfl.createAuditNKs(
                    dataset='trg_' + tableName,
                    desc='Collapse the audit columns into their NK')

                dfl.write(
                    dataset='trg_' + tableName,
                    targetTableName='trg_' + tableName,
                    dataLayerID='STG')
