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

        self.fileNameMap = {}
        self.nextFilePrefix = 1
        self.filePrefixLength = 4

    def populateFileNameMap(self):
        for root, directories, filenames in os.walk(self.tmpDataPath):
            for filename in filenames:
                _filename = filename[self.filePrefixLength+1:]
                shortfn, ext = os.path.splitext(filename)
                if ext == '.csv':
                    thisPrefix = int(filename[:self.filePrefixLength])
                    if thisPrefix >= self.nextFilePrefix:
                        self.nextFilePrefix = thisPrefix + 1
                    self.fileNameMap[_filename] = filename

    def writeDataToCsv(self, df, path, filename, headers, mode):

        _filename = ''

        if filename in self.fileNameMap:
            _filename = self.fileNameMap[filename]
        else:
            prefix = str(self.nextFilePrefix).zfill(self.filePrefixLength)
            _filename = prefix + "-" + filename
            self.nextFilePrefix += 1
            self.fileNameMap[filename] = _filename

        _file = open(path + _filename, mode)

        # If we're appending, we never put the column headers in
        colHeaders = headers
        if mode == 'a':
            colHeaders = None

        df.to_csv(_file, header=colHeaders, index=False)

    def truncateFile(self, path, filename):

        _filename = ''
        if filename in self.fileNameMap:
            _filename = self.fileNameMap[filename]
            if os.path.exists(path + _filename):
                _file = open(path + _filename, 'w')
                _file.close()

    def readDataFromCsv(self,
                        path,
                        filename,
                        sep=',',
                        quotechar='"',
                        nrows=None,
                        isTmpData=True,
                        rowNum=None):

        _filename = filename
        if isTmpData:
            # TODO: Catch missing files and raise. E.g. likely cause: running
            # transform without running extracts first.
            _filename = self.fileNameMap[filename]

        # We need to force it to read everything as text. Only way I can
        # see to do this is to read the headers and setup a dtype for each
        headersDf = pd.read_csv(path + _filename,
                                sep=sep,
                                quotechar=quotechar,
                                nrows=1,
                                na_filter=False)

        headerList = headersDf.columns.values
        dtype = {}
        for header in headerList:
            dtype[header] = str

        if rowNum is not None:
            nrows = 1
            rowNum = rowNum - 1

        return pd.read_csv(path + _filename,
                           sep=sep,
                           quotechar=quotechar,
                           dtype=dtype,
                           na_filter=False,
                           nrows=nrows,
                           skiprows=rowNum)

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
