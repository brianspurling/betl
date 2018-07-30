import os
import shutil
import tempfile
from betl.logger import logger


class Exe():

    def __init__(self, params):
        self.LOG_LEVEL = params['LOG_LEVEL']

        self.SKIP_WARNINGS = params['SKIP_WARNINGS']

        self.BULK_OR_DELTA = params['BULK_OR_DELTA']

        self.RUN_RESET = params['RUN_RESET']
        self.READ_SRC = params['READ_SRC']
        self.FAIL_LAST_EXEC = params['FAIL_LAST_EXEC']

        self.RUN_REBUILDS = params['RUN_REBUILDS']

        self.RUN_EXTRACT = params['RUN_EXTRACT']
        self.RUN_TRANSFORM = params['RUN_TRANSFORM']
        self.RUN_LOAD = params['RUN_LOAD']
        self.RUN_DM_LOAD = params['RUN_DM_LOAD']
        self.RUN_FT_LOAD = params['RUN_FT_LOAD']
        self.RUN_SUMMARISE = params['RUN_SUMMARISE']

        self.DELETE_TMP_DATA = params['DELETE_TMP_DATA']

        self.RUN_DATAFLOWS = params['RUN_DATAFLOWS']

        self.WRITE_TO_ETL_DB = params['WRITE_TO_ETL_DB']

        self.DATA_LIMIT_ROWS = params['DATA_LIMIT_ROWS']

        if self.DATA_LIMIT_ROWS:
            self.MONITOR_MEMORY_USAGE = False
        else:
            self.MONITOR_MEMORY_USAGE = True

    def deleteTempoaryData(self, tmpDataPath):

        path = tmpDataPath.replace('/', '')

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

        logger.logClearTempDataFinish()
