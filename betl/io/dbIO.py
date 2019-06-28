import pandas as pd
import numpy as np
import psycopg2
import io


def readDataFromDB(tableName, dataStore, cols='*', limitdata=None):

    if limitdata is not None:
        limitText = ' LIMIT ' + str(limitdata)
    else:
        limitText = ''

    conn = dataStore.conn

    schema = ''
    if dataStore.schema is not None:
        schema = dataStore.schema + '.'

    return pd.read_sql('SELECT ' + cols + ' FROM ' + schema +
                       tableName + limitText, con=conn)


def writeDataToDB(df, tableName, eng, if_exists,
                  emptyStringToNaN=True, dtype=None):

    if emptyStringToNaN:
        df.replace('', np.nan, inplace=True)

    connection = eng.raw_connection()
    cur = connection.cursor()
    output = io.StringIO()
    df.to_csv(output, sep=',', quotechar='"', header=False, index=False)
    output.seek(0)
    sql = 'COPY ' + tableName + ' FROM STDIN (FORMAT \'csv\', DELIMITER \',\', QUOTE \'"\')'
    cur.copy_expert(sql, output)
    # cur.copy_from(output, tableName, sep='~', null='')
    connection.commit()
    cur.close()
    # df.to_sql(tableName,
    #           eng,
    #           if_exists=if_exists,
    #           index=False)


def truncateTable(tableName, datastore, schema):
    if schema is not None:
        schema = schema + '.'
    truncateStatement = 'TRUNCATE ' + schema + tableName + ' RESTART IDENTITY'
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
        if len(df) > 0:
            df.columns = columns
    except psycopg2.ProgrammingError:
        df = None
        # TODO: can't remember putting this in, but doesn't seem right!
        # Probably because fethall won't work if you do e.g. an update
        # statement? Would be nice to pull back the table you've updated
        # instead, but how many cases to cover here?

    return df
