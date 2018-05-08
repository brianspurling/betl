from . import api as betl
from . import logger

JOB_LOG = logger.getLogger()


#
# The default summarise process. This is very simple, because summaries
# have to be custom-built by the app. All this does is truncate all the
# tables when running a bulk load
#
def defaultSummarisePrep(scheduler):

    sumLayer = scheduler.conf.getLogicalDataModel('SUM')

    sumTables = sumLayer.dataModels['SUM'].tables
    nonDefaultTrgTables = \
        scheduler.conf.schedule.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    if scheduler.bulkOrDelta == 'BULK':
        dfl = betl.DataFlow(
            desc="If it's a bulk load, drop the indexes to speed up " +
                 "writing.")
        for tableName in sumTables:
            if (sumTables[tableName].getTableType() == 'SUMMARY'):
                if tableName not in nonDefaultTrgTables:
                    for sql in sumTables[tableName].getSqlDropIndexes():
                        dfl.customSQL(
                            sql,
                            dataLayer='SUM',
                            desc='Dropping indexes for ' + tableName)

                    dfl.truncate(
                        dataset=tableName,
                        dataLayerID='SUM',
                        forceDBWrite=True,
                        desc='Because it is a bulk load, clear out the data ' +
                             '(which also restarts the SK sequences)')


def defaultSummariseFinish(scheduler):

    sumLayer = scheduler.conf.getLogicalDataModel('SUM')

    sumTables = sumLayer.dataModels['SUM'].tables
    nonDefaultTrgTables = \
        scheduler.conf.schedule.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    if scheduler.bulkOrDelta == 'BULK':
        dfl = betl.DataFlow(
            desc="If it's a bulk load, drop the indexes to speed up writing.")

        for tableName in sumTables:
            if (sumTables[tableName].getTableType() == 'SUMMARY'):
                if tableName not in nonDefaultTrgTables:
                    for sql in sumTables[tableName].getSqlCreateIndexes():
                        dfl.customSQL(
                            sql,
                            dataLayer='SUM',
                            desc='Dropping indexes for ' + tableName)
