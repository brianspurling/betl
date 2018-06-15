from . import main
from . import logger

JOB_LOG = logger.getLogger()


#
# The default summarise process. This is very simple, because summaries
# have to be custom-built by the app. All this does is truncate all the
# tables when running a bulk load
#
def defaultSummarisePrep(scheduler):

    sumLayer = scheduler.conf.data.getLogicalDataModel('SUM')

    sumTables = sumLayer.dataModels['SUM'].tables
    nonDefaultTrgTables = \
        scheduler.conf.schedule.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    if scheduler.conf.exe.BULK_OR_DELTA == 'BULK':
        dfl = main.DataFlow(
            desc="If it's a bulk load, drop the indexes to speed up " +
                 "writing.")
        for tableName in sumTables:
            if (sumTables[tableName].getTableType() == 'SUMMARY'):
                if tableName not in nonDefaultTrgTables:
                    counter = 0
                    for sql in sumTables[tableName].getSqlDropIndexes():
                        counter += 1
                        dfl.customSQL(
                            sql,
                            dataLayer='SUM',
                            desc='Dropping indexes for ' + tableName + ' (' +
                                 str(counter) + ')')

                    dfl.truncate(
                        dataset=tableName,
                        dataLayerID='SUM',
                        forceDBWrite=True,
                        desc='Because it is a bulk load, clear out the data ' +
                             'from ' + tableName + ' (which also restarts ' +
                             'the SK sequences)')

        dfl.close()


def defaultSummariseFinish(scheduler):

    pass
