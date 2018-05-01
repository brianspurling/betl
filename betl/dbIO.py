import pandas as pd
import numpy as np
from . import logger as logger
import psycopg2
from sqlalchemy.types import Text


def writeDataToDB(df, tableName, eng, if_exists,
                  emptyStingToNaN=True, dtype=None):

    if emptyStingToNaN:
        df.replace('', np.nan, inplace=True)

    df.to_sql(tableName,
              eng,
              if_exists=if_exists,
              index=False,
              dtype={col_name: Text for col_name in df})
    # TODO: can't do the above line for everything, surely?!
    # It will clash with pre-made tables and overwrite them (I think)


def readDataFromDB(tableName, conn, cols='*', limitdata=None):

    if limitdata is not None:
        limitText = ' LIMIT ' + str(limitdata)
    else:
        limitText = ''
    return pd.read_sql('SELECT ' + cols + ' FROM ' +
                       tableName + limitText, con=conn)


def customSQL(sql, datastore):
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


def truncateTable(tableName, datastore):
    truncateStatement = 'TRUNCATE ' + tableName + ' RESTART IDENTITY'
    trgDbCursor = datastore.cursor()
    trgDbCursor.execute(truncateStatement)
    datastore.commit()
