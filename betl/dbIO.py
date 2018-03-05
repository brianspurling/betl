import pandas as pd
from . import logger as logger


class DatabaseIO():

    def __init__(self, conf):

        self.devLog = logger.getDevLog(__name__)
        self.jobLog = logger.getJobLog()

        self.conf = conf

    def writeDataToDB(self, df, tableName, eng, if_exists):

        df.to_sql(tableName,
                  eng,
                  if_exists=if_exists,
                  index=False)

    def readDataFromDB(self, tableName, conn, cols='*'):

        return pd.read_sql('SELECT ' + cols + ' FROM ' + tableName, con=conn)
