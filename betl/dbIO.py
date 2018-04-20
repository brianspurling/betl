import pandas as pd
import numpy as np
from . import logger as logger
import psycopg2


class DatabaseIO():

    def __init__(self, conf):

        self.devLog = logger.getDevLog(__name__)
        self.jobLog = logger.getJobLog()

        self.conf = conf

    def writeDataToDB(self, df, tableName, eng, if_exists,
                      emptyStingToNaN=True):

        if emptyStingToNaN:
            df.replace('', np.nan, inplace=True)

        df.to_sql(tableName,
                  eng,
                  if_exists=if_exists,
                  index=False)

    def readDataFromDB(self, tableName, conn, cols='*'):

        return pd.read_sql('SELECT ' + cols + ' FROM ' + tableName, con=conn)

    def customSql(self, sql, datastore):
        dbCursor = datastore.cursor()
        dbCursor.execute(sql)
        datastore.commit()

        try:
            data = dbCursor.fetchall()
            columns = [column[0] for column in dbCursor.description]
            df = pd.DataFrame(data)
            logger.describeDataFrame(df)
            if len(df) > 0:
                df.columns = columns
        except psycopg2.ProgrammingError:
            df = None

        return df
