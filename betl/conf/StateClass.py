import datetime
import os


class State():

    def __init__(self):

        self.EXEC_ID = None
        self.RERUN_PREV_JOB = False

        # Global state for which stage (E,T,L,S) we're on, and for which
        # function of the execution's schedule we're in
        self.STAGE = 'STAGE NOT SET'
        self.FUNCTION_ID = None

        # These dictate the start/end dates of dm_date. They can be overridden
        # at any point in the application's ETL process, providing the
        # generateDMDate function is added to the schedule _after_ the
        # functions in which they're set
        self.EARLIEST_DATE_IN_DATA = datetime.date(1900, 1, 1)
        self.LATEST_DATE_IN_DATA = (datetime.date.today() +
                                    datetime.timedelta(days=365))

        self.FILE_NAME_MAP = {}
        self.nextFilePrefix = 1
        self.filePrefixLength = 4

    # For easier access, temp data files are prefixed with an incrementing
    # number when they are saved to disk. To easily access files during
    # execution, we maintain a file name mapping from the filename the code
    # expects to the actual filename on disk
    def populateFileNameMap(self, tmpDataPath):
        for root, directories, filenames in os.walk(tmpDataPath):
            for filename in filenames:
                _filename = filename[self.filePrefixLength+1:]
                shortfn, ext = os.path.splitext(filename)
                if ext == '.csv':
                    thisPrefix = int(filename[:self.filePrefixLength])
                    if thisPrefix >= self.nextFilePrefix:
                        self.nextFilePrefix = thisPrefix + 1
                    self.FILE_NAME_MAP[_filename] = filename

    def setStage(self, stage):
        self.STAGE = stage

    def setFunctionId(self, functionId):
        self.FUNCTION_ID = functionId
