import os


# Setup will have already been completed by the session-scope/autouse
# fixture in conftest.py. This set of tests checks that the setup object's
# attributes have been set correctly and that the setup itself has completed
# correctly

def test_setup(standardSetup, standardSetupParams):

    # TODO: need to do tests for creating databases, schemaDescGSheets,
    # MDMGSheets and defaultRowsGSheets

    # Check the class attributes have been set correctly. There are other
    # tests to check the set functions properly, so this test is simply
    # checking that the parameters are passed through to the correct
    # class attributes

    p = standardSetupParams
    ss = standardSetup

    assert ss.DWH_ID == p['dwhId']

    assert ss.GOOGLE_API_KEY_FILENAME == p['apiKeyFilename']
    assert ss.GOOGLE_ACCOUNT == p['googleAccount']

    assert ss.ADMIN_POSTGRES_USERNAME == p['adminPostgresUsername']
    assert ss.ADMIN_POSTGRES_PASSWORD == p['adminPostgresPassword']

    assert ss.APP_ROOT_PATH == p['appRootPath']
    assert ss.TMP_DATA_PATH == p['appRootPath'] + '/' + p['tmpDataPath']
    assert ss.SRC_DATA_PATH == p['appRootPath'] + '/' + p['srcDataPath']
    assert ss.REPORTS_PATH == p['appRootPath'] + '/' + p['reportsPath']
    assert ss.LOG_PATH == p['appRootPath'] + '/' + p['logsPath']
    assert ss.SCHEMA_PATH == p['appRootPath'] + '/' + p['schemaPath']

    assert ss.SCHEMA_DESC_ETL_GSHEET_TITLE == p['etlGSheetTitle']
    assert ss.SCHEMA_DESC_TRG_GSHEET_TITLE == p['trgGSheetTitle']

    assert ss.ETL_DB_HOST_NAME == p['etlDBHostName']
    assert ss.ETL_DB_NAME == p['etlDBName']
    assert ss.ETL_DB_USERNAME == p['etlDBUsername']
    assert ss.ETL_DB_PASSWORD == p['etlDBPassword']

    assert ss.TRG_DB_HOST_NAME == p['trgDBHostName']
    assert ss.TRG_DB_NAME == p['trgDBName']
    assert ss.TRG_DB_USERNAME == p['trgDBUsername']
    assert ss.TRG_DB_PASSWORD == p['trgDBPassword']

    assert ss.DEFAULT_ROWS_GHSEET_TITLE == p['defaultRowsGSheetTitle']
    assert ss.MDM_GHSEET_TITLE == p['mdmGSheetTitle']

    assert os.path.isdir(p['appRootPath'])
    assert os.path.isdir(p['appRootPath'] + '/' + p['tmpDataPath'])
    assert os.path.isdir(p['appRootPath'] + '/' + p['srcDataPath'])
    assert os.path.isdir(p['appRootPath'] + '/' + p['reportsPath'])
    assert os.path.isdir(p['appRootPath'] + '/' + p['logsPath'])
    assert os.path.isdir(p['appRootPath'] + '/' + p['schemaPath'])

    assert os.path.isfile(p['appRootPath'] + '/' + '.gitignore')
    assert os.path.isfile(p['appRootPath'] + '/' + 'appConfig.ini')
    assert os.path.isfile(p['appRootPath'] + '/' + 'main.py')
    assert os.path.isfile(p['appRootPath'] + '/' + 'dfl_example.py')
