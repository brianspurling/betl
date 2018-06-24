import pandas as pd
from datetime import date, timedelta


def getSchemaDescription():

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


def transformDMDate(betl):

    # TODO ideally this would be built within BETL (set a good example, and
    # all that!)
    startDate = betl.CONF.STATE.EARLIEST_DATE_IN_DATA
    endDate = betl.CONF.STATE.LATEST_DATE_IN_DATA

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

    df = pd.DataFrame(dmDateList)

    dfl = betl.DataFlow(desc='Generate the dm_date rows')

    dfl.createDataset(
        dataset='trg_dm_date',
        data=df,
        desc='Loaded a pre-constructed dataframe')

    dfl.write(
        dataset='trg_dm_date',
        targetTableName='trg_dm_date',
        dataLayerID='STG')


def getDefaultRows():

    return[{
        'date_id': -1,
        'date_yyyymmdd': 1,  # Natural key
        'cal_date': 'MISSING',
        'cal_day': None,
        'cal_month': None,
        'cal_year': None,
        'day_of_week_sunday_0_monday_1': None,
        'day_of_week_sunday_1_monday_2': None,
        'day_of_week_sunday_6_monday_0': None,
        'day_of_week_sunday_7_monday_1': None,
        'day_number': None,
        'week_number': None},

        {
        'date_id': -2,
        'date_yyyymmdd': 2,  # Natural key
        'cal_date': 'UNRECOGNISED',
        'cal_day': None,
        'cal_month': None,
        'cal_year': None,
        'day_of_week_sunday_0_monday_1': None,
        'day_of_week_sunday_1_monday_2': None,
        'day_of_week_sunday_6_monday_0': None,
        'day_of_week_sunday_7_monday_1': None,
        'day_number': None,
        'week_number': None}]
