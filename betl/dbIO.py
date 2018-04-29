import pandas as pd
import numpy as np
from . import logger as logger
import psycopg2


def writeDataToDB(df, tableName, eng, if_exists,
                  emptyStingToNaN=True):

    if emptyStingToNaN:
        df.replace('', np.nan, inplace=True)

    df.to_sql(tableName,
              eng,
              if_exists=if_exists,
              index=False)


def readDataFromDB(tableName, conn, cols='*', testDataLimit=None):

    if testDataLimit is not None:
        limitText = ' LIMIT ' + str(testDataLimit)
    else:
        limitText = ''
    return pd.read_sql('SELECT ' + cols + ' FROM ' +
                       tableName + limitText, con=conn)


def customSql(sql, datastore):
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
