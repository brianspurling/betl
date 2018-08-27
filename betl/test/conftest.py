import pytest

import betl
from betl.conf.utils import processArgs
from betl.conf import Conf
from betl.logger import Logger


@pytest.fixture(scope='session', autouse=True)
def standardSetup(standardSetupParams):
    return betl.setupBetl(standardSetupParams)


@pytest.fixture(scope='session')
def conf(standardSetupParams, standardRunTimeParams, standardScheduleConfig):
    conf = Conf(
        appConfigFile=standardSetupParams['appRootPath'] + '/appConfig.ini',
        runTimeParams=processArgs(standardRunTimeParams),
        scheduleConfig=standardScheduleConfig)
    Logger.initialiseLogging(
        execId=conf.STATE.EXEC_ID,
        logLevel=conf.EXE.LOG_LEVEL,
        logPath=conf.CTRL.LOG_PATH,
        auditCols=conf.DATA.AUDIT_COLS)
    return conf


@pytest.fixture(scope='session')
def standardSetupParams():
    params = {
        'dwhId': 'TST',
        'apiKeyFilename': 'betl_test_google_api_key.json',
        'googleAccount': 'brian.spurling@gmail.com',
        'adminPostgresUsername': 'b_spurling',
        'adminPostgresPassword': '',
        'appRootPath': 'test_app',
        'tmpDataPath': 'tmp_data',
        'srcDataPath': 'src_data',
        'reportsPath': 'reports',
        'logsPath': 'logs',
        'schemaPath': 'schema',
        'ctlDBHostName': 'localhost',
        'ctlDBName': 'tst_ctl',
        'ctlDBUsername': 'b_spurling',
        'ctlDBPassword': '',
        'etlGSheetTitle': 'TST - ETL DB SCHMEA',
        'trgGSheetTitle': 'TST - TRG DB SCHMEA',
        'etlDBHostName': 'localhost',
        'etlDBName': 'tst_etl',
        'etlDBUsername': 'b_spurling',
        'etlDBPassword': '',
        'trgDBHostName': 'localhost',
        'trgDBName': 'tst_trg',
        'trgDBUsername': 'b_spurling',
        'trgDBPassword': '',
        'defaultRowsGSheetTitle': 'TST - Default Rows',
        'mdmGSheetTitle': 'TST - Master Data Mappings',
        'createDirectories': 'Y',
        'createGitignoreFile': 'Y',
        'createAppConfigFile': 'Y',
        'createMainScript': 'Y',
        'createExampleDataflow': 'Y',
        'createDatabases': 'Y',
        'createSchemaDescGSheets': 'N',
        'createMDMGsheet': 'N',
        'createDefaultRowsGsheet': 'N'
    }
    return params


@pytest.fixture(scope='session')
def standardRunTimeParams():
    runTimeParams = [
        'main.py',
        'bulk',
        'run',
        'nowarnings',
        'limitdata']  # ensures memory monitoring is off
    return runTimeParams


@pytest.fixture(scope='session')
def standardScheduleConfig():

    scheduleConfig = {
        'DEFAULT_EXTRACT': True,
        'DEFAULT_LOAD': True,
        'DEFAULT_SUMMARISE': False,
        'DEFAULT_DM_DATE': False,
        'DEFAULT_DM_AUDIT': True,
        'EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],
        'BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],
        'EXTRACT_DATAFLOWS': [],
        'TRANSFORM_DATAFLOWS': [],
        'LOAD_DATAFLOWS': [],
        'SUMMARISE_DATAFLOWS': []
    }

    return scheduleConfig
