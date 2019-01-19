import os
import shutil
import tempfile
import json
import ast
import datetime
import time
import sys

from betl.logger import alerts
from betl.io import fileIO
from betl.logger import Logger
from betl.ControlDBClass import ControlDB
# TODO I'd rather not import these next two directly, but just import the
# whole betlutils and reference as betlutils.cleanTableName etc
from betl.datamodel import DataLayer
from betl.logger import cliText
from betl.scheduler import Scheduler

# TODO: I suspect some of these this will be surpassed by Airflow
# functionality, but maybe it's in the interest of the framework to keep
# bespoke control tables, along with the logging etc. Not neccessarily the
# case that Airflow will be better than this...

###################
# INITIALISE BETL #
###################

def initBETL(betl):

    betl.LOG = Logger(betl)
    betl.LOG.logBETLStart(betl)
    betl.CONTROL_DB = ControlDB(betl)

    if betl.CONF.RUN_RESET:

        betl.LOG.logResetStart()

        betl.CONTROL_DB.dropAllCtlTables()
        betl.CONTROL_DB.createExecutionsTable()
        betl.CONTROL_DB.createFunctionsTable()
        betl.CONTROL_DB.createDataflowsTable()
        betl.CONTROL_DB.createStepsTable()

        archiveLogFiles(betl)
        createReportsDir(betl)
        createSchemaDir(betl)

        betl.LOG.logResetEnd()


def archiveLogFiles(betl):
    # Archive Log Files

    timestamp = datetime.datetime.fromtimestamp(
        time.time()
    ).strftime('%Y%m%d%H%M%S')

    source = betl.CONF.LOG_PATH
    dest = betl.CONF.LOG_PATH + '/archive_' + timestamp + '/'

    if not os.path.exists(dest):
        os.makedirs(dest)

    files = os.listdir(source)
    for file in files:
        if file.find('jobLog') > -1:
            shutil.move(source + '/' + file, dest)
        if file.find('alerts') > -1:
            shutil.move(source + '/' + file, dest)


def createReportsDir(betl):
    if (os.path.exists(betl.CONF.REPORTS_PATH)):
        tmp = tempfile.mktemp(dir=os.path.dirname(betl.CONF.REPORTS_PATH))
        shutil.move(betl.CONF.REPORTS_PATH, tmp)  # rename
        shutil.rmtree(tmp)  # delete
    os.makedirs(betl.CONF.REPORTS_PATH)  # create the new folder


def createSchemaDir(betl):
    # The mapping file is created on auto-pop, i.e. once, so we need
    # to preserve it
    srcTableNameMappingFile = betl.CONF.SCHEMA_PATH + '/srcTableNameMapping.txt'
    tableNameMap = None
    try:
        mapFile = open(srcTableNameMappingFile, 'r')
        tableNameMap = ast.literal_eval(mapFile.read())
    except FileNotFoundError:
        pass

    if os.path.exists(betl.CONF.SCHEMA_PATH + '/'):
        shutil.rmtree(betl.CONF.SCHEMA_PATH + '/')
    os.makedirs(betl.CONF.SCHEMA_PATH + '/')
    open(betl.CONF.SCHEMA_PATH + '/lastModifiedTimes.txt', 'a').close()

    if tableNameMap is not None:
        with open(srcTableNameMappingFile, 'w+') as file:
            file.write(json.dumps(tableNameMap))


def validateSchedule(betl):
    # Check that bulk/delta set correctly
    if betl.CONF.RUN_DATAFLOWS and \
       ((bulk and delta) or ((not bulk) and (not delta))):
        raise ValueError(cliText.BULK_OR_DELTA_NOT_SET)
    elif betl.CONF.RUN_DATAFLOWS and bulk:
        if not skipWarnings:
            text = input(cliText.BULK_LOAD_WARNING)
            if text.lower() != 'y':
                sys.exit()
            else:
                print('')
    if not params['WRITE_TO_ETL_DB'] and params['RUN_TESTS']:
        raise ValueError(cliText.TURN_OFF_TESTS_IF_NOT_WRITING_TO_DB)

#######################
# SETUP STAGE LOGGING #
#######################

def logSetupStart(betl):
    betl.LOG.logSetupStart()


def logSetupEnd(betl):
    betl.LOG.logSetupEnd()


########################
# TEMPORARY DATA SETUP #
########################

# For easier manual access, temp data files are prefixed with an incrementing
# number when they are saved to disk. So to easily access files during
# execution, we maintain a file name mapping:
# filename the code expects --> the actual filename on disk (ie incl prefix)
def populateTempDataFileNameMap(betl):

    for root, directories, filenames in os.walk(betl.CONF.TMP_DATA_PATH):
        for filename in filenames:
            _filename = filename[betl.CONF.FILE_PREFIX_LENGTH+1:]
            shortfn, ext = os.path.splitext(filename)
            if ext == '.csv':
                thisPrefix = int(filename[:betl.CONF.FILE_PREFIX_LENGTH])
                if thisPrefix >= betl.CONF.NEXT_FILE_PREFIX:
                    betl.CONF.NEXT_FILE_PREFIX = thisPrefix + 1
                betl.CONF.FILE_NAME_MAP[_filename] = filename

    betl.LOG.logPopulateTempDataFileNameMapEnd()


#
# OTHER
#

def cleanTableName(tableName_src):
    tableName = tableName_src
    tableName = tableName.replace(' ', '_')
    tableName = tableName.replace('(', '')
    tableName = tableName.replace(')', '')
    tableName = tableName.replace('-', '')
    tableName = tableName.lower()
    return tableName
