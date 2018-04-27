import pandas as pd
from . import logger as logger


class GsheetIO():

    def __init__(self, conf):

        self.devLog = logger.getDevLog(__name__)
        self.jobLog = logger.getJobLog()

        self.conf = conf

    def readDataFromWorksheet(self, worksheet, testDataLimit=None):

        data = worksheet.get_all_values()
        if testDataLimit is not None:
            rowLimit = len(data)
        else:
            rowLimit = testDataLimit

        return pd.DataFrame(data[1:rowLimit], columns=data[0])
