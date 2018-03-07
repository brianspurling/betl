import pandas as pd
from datetime import date, timedelta

# from . import logger
from . import api


def getSchemaDescription():

    # TODO: Surely some should be date types?! Don't forget to fis default
    # rows too

    # This schema description reflects the same meta data structure that
    # we find in the schema spreadsheets.
    tableSchema = {
        'tableName': 'dm_date',
        'columnSchemas': {}
    }

    tableSchema['columnSchemas']['date_id'] = {
        'tableName':   'dm_date',
        'columnName':  'date_id',
        'dataType':    'INTEGER',
        'columnType':  'Surrogate key',
        'fkDimension': None
    }

    tableSchema['columnSchemas']['dateYYYYMMDD'] = {
        'tableName':   'dm_date',
        'columnName':  'date_yyyymmdd',
        'dataType':    'INTEGER',
        'columnType':  'Natural key',
        'fkDimension': None
    }

    attrColumns = [
        'cal_date',
        'cal_day',
        'cal_month',
        'cal_year',
        'day_of_week_sunday_0_monday_1',
        'day_of_week_sunday_1_monday_2',
        'day_of_week_sunday_6_monday_0',
        'day_of_week_sunday_7_monday_1',
        'day_number',
        'week_number']

    for colName in attrColumns:

        tableSchema['columnSchemas'][colName] = {
            'tableName':   'dm_date',
            'columnName':  colName,
            'dataType':    'TEXT',
            'columnType':  'Attribute',
            'fkDimension': None
        }

    return tableSchema


def transformDMDate(scheduler):

    # to do #9
    # TODO # 51

    startDate = scheduler.conf.state.EARLIEST_DATE_IN_DATA
    endDate = scheduler.conf.state.LATEST_DATE_IN_DATA

    dmDateList = []
    while startDate <= endDate:

        dateInfo = {'date_id': startDate.strftime('%Y%m%d'),
                    'date_yyyymmdd': startDate.strftime('%Y%m%d'),
                    'cal_date': startDate.strftime('%Y-%m-%d'),
                    'cal_day': startDate.day,
                    'cal_month': startDate.month,
                    'cal_year': startDate.year}
        dateInfo['day_of_week_sunday_0_monday_1'] = startDate.isoweekday() % 7
        dateInfo['day_of_week_sunday_1_monday_2'] = \
            startDate.isoweekday() % 7 + 1
        dateInfo['day_of_week_sunday_6_monday_0'] = startDate.weekday()
        dateInfo['day_of_week_sunday_7_monday_1'] = startDate.isoweekday()
        dateInfo['day_number'] = startDate.toordinal() - \
            date(startDate.year - 1, 12, 31).toordinal()
        dateInfo['week_number'] = startDate.isocalendar()[1]

        dmDateList.append(dateInfo)

        startDate = startDate + timedelta(1)

    dateInfo = {'date_id': -1,
                'date_yyyymmdd': 00000000,  # Natural key
                'cal_date': 'MISSING',
                'cal_day': None,
                'cal_month': None,
                'cal_year': None,
                'day_of_week_sunday_0_monday_1': None,
                'day_of_week_sunday_1_monday_2': None,
                'day_of_week_sunday_6_monday_0': None,
                'day_of_week_sunday_7_monday_1': None,
                'day_number': None,
                'week_number': None}

    dmDateList.append(dateInfo)

    dateInfo = {'date_id': -2,
                'date_yyyymmdd': 00000000,  # Natural key
                'cal_date': 'UNRECOGNISED',
                'cal_day': None,
                'cal_month': None,
                'cal_year': None,
                'day_of_week_sunday_0_monday_1': None,
                'day_of_week_sunday_1_monday_2': None,
                'day_of_week_sunday_6_monday_0': None,
                'day_of_week_sunday_7_monday_1': None,
                'day_number': None,
                'week_number': None}

    dmDateList.append(dateInfo)

    df = pd.DataFrame(dmDateList)

    api.writeData(df, 'trg_dm_date', 'STG')

    del df
