import pandas as pd
from . import logger as logger


class GsheetIO():

    def __init__(self, conf):

        self.devLog = logger.getDevLog(__name__)
        self.jobLog = logger.getJobLog()

        self.conf = conf

    def readDataFromWorksheet(self, worksheet):

        data = worksheet.get_all_values()
        return pd.DataFrame(data[1:], columns=data[0])
