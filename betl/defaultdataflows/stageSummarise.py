def logSummariseStart(conf):
    conf.log('logSummariseStart')


def logBespokeSummariseStart(conf):
    conf.log('logSummariseStart')


def logBespokeSummariseEnd(conf):
    conf.log('logSummariseStart')


def logSummariseEnd(conf):
    conf.log('logSummariseEnd')


def logSkipSummarise(conf):
    conf.log('logSkipSummarise')


#
# The default summarise process. This is very simple, because summaries
# have to be custom-built by the app. All this does is truncate all the
# tables when running a bulk load
#
def defaultSummarisePrep(conf):

    sumLayer = conf.getLogicalSchemaDataLayer('SUM')

    sumTables = sumLayer.datasets['SUM'].tables
    nonDefaultTrgTables = \
        conf.BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    if conf.BULK_OR_DELTA == 'BULK':
        dfl = conf.DataFlow(
            desc="If it's a bulk load, drop the indexes to speed up writing.")
        for tableName in sumTables:
            if (sumTables[tableName].getTableType() == 'SUMMARY'):
                if tableName not in nonDefaultTrgTables:
                    counter = 0
                    for sql in sumTables[tableName].getSqlDropIndexes():
                        counter += 1
                        dfl.customSQL(
                            sql,
                            databaseID='TRG',
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
