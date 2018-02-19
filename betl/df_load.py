from . import logger
from . import schemas
from . import conf

import pandas as pd

log = logger.setUpLogger('EXTRCT', __name__)


#
# A default load process. Bulk is obvious and as you would expect
# Delta deals with SCD et al
# This function assumes that dm_a_dimension is loaded from a csv file
# called dm_a_dimension.csv. If that isn't the case, pass in to
# nonDefaultStagingTables a key,value pair of <dimension name>,<staging csv>
#
def defaultLoad():
    nonDefaultStagingTables = conf.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    # If it's a bulk load, clear out all the target tables (which also
    # restarts the PK sequences
    if conf.BULK_OR_DELTA == 'BULK':
        schemas.TRG_LAYER.truncatePhysicalDataModel()

    # TODO: move all this looping crap into the schemas, like the seq resets
    # TODO: this is fine for dimensions, but we need to pick out the
    # facts and handle them differently - i.e. with SK lookups
    # So let's get these functions embedded into schemas, but with
    # conditions on fact/dim delta/bulk, and then bulid fact/bulk.
    #   - I need the dim PKs in memory... for bulk, perhaps we generate
    #     in python then write to DB. Messy. Otherwise, I think we need to
    #     write the dims then read them back out :(
    #   - Add to google sheet (TRG DB Schema) the name of the dim for each FK
    #   - Load dim, load fact.
    #   - For each fact column, if it's a FK, lookup what its dim is from
    #     GSheet, join to dim's natural key column (some of these are missing
    #     in GSheet), then write dim's ID col to fact["fk_" + <col name>]
    for dataModelId in schemas.TRG_LAYER.dataModels:
        for tableName in schemas.TRG_LAYER.dataModels[dataModelId].tables:
            if tableName not in nonDefaultStagingTables:
                if conf.BULK_OR_DELTA == 'BULK':
                    bulkLoadTable(tableName)
                elif conf.BULK_OR_DELTA == 'DELTA':
                    bulkLoadTable(tableName)


def bulkLoadTable(tableName):
    eng = conf.TRG_DB_ENG

    logger.logStepStart('Get data from staging csv: ' + tableName + '.csv', 1)
    df = pd.read_csv(conf.TMP_DATA_PATH + tableName + '.csv')
    logger.logStepEnd(df)

    # We can append rows, because, as we're running a bulk load, we will
    # have just cleared out the TRG model and created. This way, append
    # guarantees we error if we don't load all the required columns
    df.to_sql(tableName, eng,
              if_exists='append',
              index=False)


def deltaLoadTable(tableName):

    raise ValueError('DELTA load functions not yet written')
