import pytest
import os

from betl.setupModule import setupUtils


@pytest.fixture
def params():
    return {
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
        'createDatabases': 'N',
        'createSchemaDescGSheets': 'N',
        'createMDMGsheet': 'N',
        'createDefaultRowsGsheet': 'N'
    }

    # TODO: need to do tests for creating databases, schemaDescGSheets,
    # MDMGSheets and defaultRowsGSheets


def test_Setup_setupWithParameters(params):

    setup = setupUtils.setup(params)

    # Check the class attributes have been set correctly. There are other
    # tests to check the set functions properly, so this test is simply
    # checking that the parameters are passed through to the correct
    # class attributes

    assert setup.DWH_ID == params['dwhId']

    assert setup.GOOGLE_API_KEY_FILENAME == params['apiKeyFilename']
    assert setup.GOOGLE_ACCOUNT == params['googleAccount']

    assert setup.ADMIN_POSTGRES_USERNAME == params['adminPostgresUsername']
    assert setup.ADMIN_POSTGRES_PASSWORD == params['adminPostgresPassword']

    assert setup.APP_ROOT_PATH == params['appRootPath']
    assert setup.TMP_DATA_PATH == params['appRootPath'] + '/' + params['tmpDataPath']
    assert setup.SRC_DATA_PATH == params['appRootPath'] + '/' + params['srcDataPath']
    assert setup.REPORTS_PATH == params['appRootPath'] + '/' + params['reportsPath']
    assert setup.LOG_PATH == params['appRootPath'] + '/' + params['logsPath']
    assert setup.SCHEMA_PATH == params['appRootPath'] + '/' + params['schemaPath']

    assert setup.CTL_DB_HOST_NAME == params['ctlDBHostName']
    assert setup.CTL_DB_NAME == params['ctlDBName']
    assert setup.CTL_DB_USERNAME == params['ctlDBUsername']
    assert setup.CTL_DB_PASSWORD == params['ctlDBPassword']

    assert setup.SCHEMA_DESC_ETL_GSHEET_TITLE == params['etlGSheetTitle']
    assert setup.SCHEMA_DESC_TRG_GSHEET_TITLE == params['trgGSheetTitle']

    assert setup.ETL_DB_HOST_NAME == params['etlDBHostName']
    assert setup.ETL_DB_NAME == params['etlDBName']
    assert setup.ETL_DB_USERNAME == params['etlDBUsername']
    assert setup.ETL_DB_PASSWORD == params['etlDBPassword']

    assert setup.TRG_DB_HOST_NAME == params['trgDBHostName']
    assert setup.TRG_DB_NAME == params['trgDBName']
    assert setup.TRG_DB_USERNAME == params['trgDBUsername']
    assert setup.TRG_DB_PASSWORD == params['trgDBPassword']

    assert setup.DEFAULT_ROWS_GHSEET_TITLE == params['defaultRowsGSheetTitle']
    assert setup.MDM_GHSEET_TITLE == params['mdmGSheetTitle']

    assert os.path.isdir(params['appRootPath'])
    assert os.path.isdir(params['appRootPath'] + '/' + params['tmpDataPath'])
    assert os.path.isdir(params['appRootPath'] + '/' + params['srcDataPath'])
    assert os.path.isdir(params['appRootPath'] + '/' + params['reportsPath'])
    assert os.path.isdir(params['appRootPath'] + '/' + params['logsPath'])
    assert os.path.isdir(params['appRootPath'] + '/' + params['schemaPath'])

    assert os.path.isfile(params['appRootPath'] + '/' + '.gitignore')
    assert os.path.isfile(params['appRootPath'] + '/' + 'appConfig.ini')
    assert os.path.isfile(params['appRootPath'] + '/' + 'main.py')
    assert os.path.isfile(params['appRootPath'] + '/' + 'dfl_example.py')
