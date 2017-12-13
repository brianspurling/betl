from . import utilities as utils
from . import conf
from . import schemas

import pandas as pd
import pandas_profiling
import datetime
import os


log = utils.setUpLogger('PROFIL', __name__)


def profileSrc():
    # Loop through all source tables
    # run profile() for each one

    src = schemas.SRC_LAYER

    for dataModelId in src.dataModels:
        conn = conf.ETL_DB_CONN
        tables = src.dataModels[dataModelId].tables
        for tableName in tables:
            table = tables[tableName]
            if tableName != 'src_ipa_bundles':
                df = pd.read_sql('SELECT * FROM ' + tableName, con=conn)
                # remove audit columns
                df = df[table.columnList_withoutAudit]
                profile(df, 'SRC', dataModelId, tableName)
                del df


#
# Output a standard profile report to HTML
#
def profile(df, dataLayer, dataModel, tableName):

    log.debug('START')

    fileName = tableName + ".html"
    filePath = "profiling/" + dataLayer + '/' + dataModel

    if not os.path.exists(filePath):
        os.makedirs(filePath)

    time = str(datetime.datetime.time(datetime.datetime.now()))
    log.info('Profiling started - ' + dataLayer + '.' + dataModel + '.'
             + tableName + ' (start: ' + time + ')')

    profile = pandas_profiling.ProfileReport(df)

    time = str(datetime.datetime.time(datetime.datetime.now()))
    log.info('Profiling completed - ' + dataLayer + '.' + dataModel + '.'
             + tableName + ' (end: ' + time + ')')

    profile.to_file(outputfile=filePath + fileName)

    log.info('Profile report exported to file: ' + filePath + fileName)

    log.debug('END')
