from datetime import date, timedelta
import pandas as pd

from .conf import EARLIEST_DATE_IN_DATA
from .conf import LATEST_DATE_IN_DATA
from . import conf


def generateDMDate(writeToDimension='True'):

    # to do: need to build up a logStr in this function, like bespoke funcs do
    logStr = ''

    startDate = EARLIEST_DATE_IN_DATA
    endDate = LATEST_DATE_IN_DATA

    dmDateList = []

    while startDate <= endDate:
        dateInfo = {'dateYYYYMMDD': startDate.strftime('%Y%m%d'),
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

    # To do: this needs to be set up as bulk only. Delta would be additional
    # dates, or maybe the ability to add new columns, but needs to be done
    # without changing SKs
    if writeToDimension:
        df.to_sql('dm_date',
                  conf.TRG_DB_ENG,
                  if_exists='replace',
                  index=False)

    return logStr
