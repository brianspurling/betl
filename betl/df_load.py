from . import utilities as utils
from . import schemas
from . import conf

import pandas as pd

log = utils.setUpLogger('EXTRCT', __name__)


#
# A default load process. Bulk is obvious and as you would expect
# Delta deals with SCD et al
# This function assumes that dm_a_dimension is loaded from a csv file
# called dm_a_dimension.csv. If that isn't the case, pass in to
# nonDefaultStagingTables a key,value pair of <dimension name>,<staging csv>
#
def defaultLoad():

    logStr = ''

    nonDefaultStagingTables = conf.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

    # If it's a bulk load, clear out all the target tables (which also
    # restarts the PK sequences
    if conf.BULK_OR_DELTA == 'BULK':
        schemas.TRG_LAYER.truncatePhysicalDataModel()

    # TODO: move all this looping crap into the schemas, like the seq resets
    for dataModelId in schemas.TRG_LAYER.dataModels:
        for tableName in schemas.TRG_LAYER.dataModels[dataModelId].tables:
            if tableName not in nonDefaultStagingTables:
                if conf.BULK_OR_DELTA == 'BULK':
                    logStr += bulkLoadTable(tableName)
                elif conf.BULK_OR_DELTA == 'DELTA':
                    logStr += bulkLoadTable(tableName)

    return logStr


def bulkLoadTable(tableName):
    funcName = 'bulkLoadTable'
    logStr = ''

    eng = conf.TRG_DB_ENG

    a = 'Get data from staging csv: ' + tableName + '.csv'
    df = pd.read_csv(conf.TMP_DATA_PATH + tableName + '.csv')
    logStr += utils.describeDF(funcName, a, df, 1)

    # We can append rows, because, as we're running a bulk load, we will
    # have just cleared out the TRG model and created. This way, append
    # guarantees we error if we don't load all the required columns
    df.to_sql(tableName, eng,
              if_exists='append',
              index=False)

    return logStr


def deltaLoadTable(tableName):
    logStr = ''

    raise ValueError('DELTA load functions not yet written')

    return logStr
