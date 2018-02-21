from . import logger
from . import schemas
from . import conf
from . import utilities as utils
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

    # If it's a bulk load, clear out all the target tables (which also
    # restarts the PK sequences
    if conf.BULK_OR_DELTA == 'BULK':
        schemas.TRG_LAYER.truncatePhysicalDataModel(
            truncDims=conf.RUN_DM_LOAD,
            truncFacts=conf.RUN_FT_LOAD)

    schemas.TRG_LAYER.load()


def deltaLoadTable(tableName):

    raise ValueError('DELTA load functions not yet written')


def loadDMDate():

    # to do #22
    # to do #57
    df = utils.readFromCsv('trg_dm_date')
    # TODO: because I read from csv as text, and because
    # there's no schema for dm_date, I'm getting text IDs etc
    # There's more than just the ID to change, but that's all i needed
    # to test the star joins. Integrating delta loads might sort this out
    # anyway
    df['date_id'] = pd.to_numeric(df.date_id, errors='coerce')
    utils.writeToTrgDB(df, 'dm_date', if_exists='replace')
    utils.retrieveSksFromDimension('dm_date', ['dateYYYYMMDD'], 'date_id')
    del df
