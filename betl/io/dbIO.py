import pandas as pd
import numpy as np
from betl.logger import logger
import psycopg2
from sqlalchemy.types import Text


def readDataFromDB(tableName, conn, cols='*', limitdata=None):

    if limitdata is not None:
        limitText = ' LIMIT ' + str(limitdata)
    else:
        limitText = ''
    return pd.read_sql('SELECT ' + cols + ' FROM ' +
                       tableName + limitText, con=conn)


def writeDataToDB(df, tableName, eng, if_exists,
                  emptyStingToNaN=True, dtype=None):

    if emptyStingToNaN:
        df.replace('', np.nan, inplace=True)

    df.to_sql(tableName,
              eng,
              if_exists=if_exists,
              index=False,
              dtype={col_name: Text for col_name in df})
    # TODO: why do I need to do the above line? It was put in to solve a bug
    # but I would have thought it wasn't ok, because it will clash with
    # pre-made tables and overwrite them. However it doesn't appear to be
    # doing that... :s


def truncateTable(tableName, datastore):
    truncateStatement = 'TRUNCATE ' + tableName + ' RESTART IDENTITY'
    trgDbCursor = datastore.cursor()
    trgDbCursor.execute(truncateStatement)
    datastore.commit()


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
        # TODO: can't remember putting this in, but doesn't seem right!

    return df
