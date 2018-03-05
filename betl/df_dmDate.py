import pandas as pd
from datetime import date, timedelta

# from . import logger
from . import api


def transformDMDate(scheduler):

    # to do #9
    # TODO # 51

    startDate = scheduler.conf.state.EARLIEST_DATE_IN_DATA
    endDate = scheduler.conf.state.LATEST_DATE_IN_DATA

    dmDateList = []
    while startDate <= endDate:

        dateInfo = {'date_id': startDate.strftime('%Y%m%d'),
                    'dateYYYYMMDD': startDate.strftime('%Y%m%d'),
                    'calDate': startDate.strftime('%Y-%m-%d'),
                    'calDay': startDate.day,
                    'calMonth': startDate.month,
                    'calYear': startDate.year}
        dateInfo['dayOfWeekSunday0Monday1'] = startDate.isoweekday() % 7
        dateInfo['dayOfWeekSunday1Monday2'] = startDate.isoweekday() % 7 + 1
        dateInfo['dayOfWeekSunday6Monday0'] = startDate.weekday()
        dateInfo['dayOfWeekSunday7Monday1'] = startDate.isoweekday()
        dateInfo['dayNumber'] = startDate.toordinal() -                       \
            date(startDate.year - 1, 12, 31).toordinal()
        dateInfo['weekNumber'] = startDate.isocalendar()[1]

        dmDateList.append(dateInfo)

        startDate = startDate + timedelta(1)

    df = pd.DataFrame(dmDateList)

    api.writeDataToCsv(df, 'trg_dm_date')

    del df


def loadDMDate(scheduler):

    # devLog = logger.getDevLog(__name__)

    # to do #22
    # to do #57
    df = api.readDataFromCsv('trg_dm_date')
    # TODO: because I read from csv as text, and because
    # there's no schema for dm_date, I'm getting text IDs etc
    # There's more than just the ID to change, but that's all i needed
    # to test the star joins. Integrating delta loads might sort this out
    # anyway
    df['date_id'] = pd.to_numeric(df.date_id, errors='coerce')
    api.writeDataToTrgDB(df, 'dm_date', if_exists='replace')
    api.getSKMapping('dm_date', ['dateYYYYMMDD'], 'date_id')
    del df
