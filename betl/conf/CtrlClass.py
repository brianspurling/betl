import datetime
import time
import os
import sys
import shutil
import tempfile
import ast
import json

from betl.logger import cliText

from betl.ctrldb import CtrlDB


class Ctrl():

    def __init__(self, config, runReset):

        self.config = config

        self.DWH_ID = self.config['DWH_ID']
        self.TMP_DATA_PATH = self.config['TMP_DATA_PATH'] + '/'
        self.REPORTS_PATH = self.config['REPORTS_PATH'] + '/'
        self.LOG_PATH = self.config['LOG_PATH'] + '/'

        dbConfigObj = self.config['ctl_db']
        self.CTRL_DB = CtrlDB(
            host=dbConfigObj['HOST'],
            dbName=dbConfigObj['DBNAME'],
            username=dbConfigObj['USER'],
            password=dbConfigObj['PASSWORD'])

        # If the run_reset parameter was passed in at the command line,
        # we rebuild the ctl database / archive old logs / etc
        if runReset:
            self.reset()

    def initExecution(self, exeConf, stateConf):

        self.createReportsDir()

        if exeConf.FAIL_LAST_EXEC:
            self.CTRL_DB.failLastExecution()

        lastExecRow = self.CTRL_DB.getLastExecution()

        lastExecDetails = {}
        if len(lastExecRow) > 0:  # in case it's the first execution!
            lastExecDetails = {'lastExecId': lastExecRow[0][0],
                               'lastExecStatus': lastExecRow[0][1]}
        else:
            lastExecDetails = {'lastExecId': None,
                               'lastExecStatus': 'NO_PREV_EXEC'}

        execId = None

        if lastExecDetails['lastExecStatus'] == 'NO_PREV_EXEC':
            execId = 1
        elif (lastExecDetails['lastExecStatus'] == 'RUNNING' and
              exeConf.RUN_DATAFLOWS and
              not exeConf.SKIP_WARNINGS):

            text = input(cliText.LAST_EXE_STILL_RUNNING)
            sys.exit()

        elif (lastExecDetails['lastExecStatus'] != 'SUCCESSFUL' and
              exeConf.RUN_DATAFLOWS and
              not exeConf.SKIP_WARNINGS):

            text = input(cliText.LAST_EXE_FAILED.format(
                status=lastExecDetails['lastExecStatus']))
            if text.lower() != 'ignore':
                if exeConf.RUN_RESET or exeConf.RUN_REBUILDS:
                    text = input(cliText.CANT_RERUN_WITH_SETUP_OR_REBUILD)
                    sys.exit()
                else:
                    stateConf.RERUN_PREV_JOB = True
                    execId = lastExecDetails['lastExecId']
            else:
                execId = lastExecDetails['lastExecId'] + 1
        else:
            execId = lastExecDetails['lastExecId'] + 1

        stateConf.EXEC_ID = execId

        if not stateConf.RERUN_PREV_JOB:
            self.CTRL_DB.insertExecution(
                execId,
                exeConf.BULK_OR_DELTA,
                exeConf.DATA_LIMIT_ROWS)

        lastExecReport = {
            'lastExecId': lastExecDetails['lastExecId'],
            'lastExecStatus': lastExecDetails['lastExecStatus'],
            'execId': execId
        }
        return lastExecReport

    def reset(self):
        self.CTRL_DB.dropAllCtlTables()
        self.CTRL_DB.createExecutionsTable()
        self.CTRL_DB.createFunctionsTable()
        self.CTRL_DB.createDataflowsTable()
        self.CTRL_DB.createStepsTable()

        self.archiveLogFiles()
        self.createReportsDir()
        self.createSchemaDir()

    def archiveLogFiles(self):

        timestamp = datetime.datetime.fromtimestamp(
            time.time()
        ).strftime('%Y%m%d%H%M%S')

        source = self.LOG_PATH
        dest = self.LOG_PATH + 'archive_' + timestamp + '/'

        if not os.path.exists(dest):
            os.makedirs(dest)

        files = os.listdir(source)
        for f in files:
            if f.find('jobLog') > -1:
                shutil.move(source+f, dest)
            if f.find('alerts') > -1:
                shutil.move(source+f, dest)

    def createSchemaDir(self):

        # The mapping file is created on auto-pop, i.e. once, so we need
        # to preserve it
        tableNameMap = None
        try:
            mapFile = open('schemas/tableNameMapping.txt', 'r')
            tableNameMap = ast.literal_eval(mapFile.read())
        except FileNotFoundError:
            pass

        shutil.rmtree('schemas/')
        os.makedirs('schemas/')
        open('schemas/lastModifiedTimes.txt', 'a').close()

        if tableNameMap is not None:
            with open('schemas/tableNameMapping.txt', 'w+') as file:
                file.write(json.dumps(tableNameMap))

    def createReportsDir(self):
        path = self.REPORTS_PATH.replace('/', '')

        if (os.path.exists(path)):
            tmp = tempfile.mktemp(dir=os.path.dirname(path))
            shutil.move(path, tmp)  # rename
            shutil.rmtree(tmp)  # delete
        os.makedirs(path)  # create the new folder
