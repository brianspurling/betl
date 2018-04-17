import tempfile
import shutil
import os
import pandas as pd
from . import logger as logger


class FileIO():

    def __init__(self, conf):

        self.devLog = logger.getDevLog(__name__)
        self.jobLog = logger.getJobLog()

        self.tmpDataPath = conf.app.TMP_DATA_PATH
        self.conf = conf

    def writeDataToCsv(self, df, path, headers, mode):

        _file = open(path, mode)

        # If we're appending, we never put the column headers in
        colHeaders = headers
        if mode == 'a':
            colHeaders = None

        df.to_csv(_file, header=colHeaders, index=False)

    def readDataFromCsv(self,
                        path,
                        sep=',',
                        quotechar='"',
                        nrows=None):

        # We need to force it to read everything as text. Only way I can
        # see to do this is to read the headers and setup a dtype for each
        headersDf = pd.read_csv(path,
                                sep=sep,
                                quotechar=quotechar,
                                nrows=1,
                                na_filter=False)

        headerList = headersDf.columns.values
        dtype = {}
        for header in headerList:
            dtype[header] = str

        return pd.read_csv(path,
                           sep=sep,
                           quotechar=quotechar,
                           dtype=dtype,
                           na_filter=False,
                           nrows=nrows)

    def deleteTempoaryData(self):
        self.devLog.info("START")

        path = self.tmpDataPath.replace('/', '')

        if (os.path.exists(path)):
            # `tempfile.mktemp` Returns an absolute pathname of a file that
            # did not exist at the time the call is made. We pass
            # dir=os.path.dirname(dir_name) here to ensure we will move
            # to the same filesystem. Otherwise, shutil.copy2 will be used
            # internally and the problem remains: we're still deleting the
            # folder when we come to recreate it
            tmp = tempfile.mktemp(dir=os.path.dirname(path))
            shutil.move(path, tmp)  # rename
            shutil.rmtree(tmp)  # delete
        os.makedirs(path)  # create the new folder
        self.jobLog.info(logger.logClearedTempData())
        self.devLog.info("END")
