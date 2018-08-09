import pytest
from betl.setupModule import Setup


@pytest.mark.parametrize("dwhId, expected", [
    ('TST', 'TST'),
    ('', 'DWH'),
    (None, 'DWH')])
def test_Setup_setDwhId(dwhId, expected):
    setup = Setup()
    setup.setDwhId(dwhId)
    assert setup.DWH_ID == expected


@pytest.mark.parametrize("apiKeyFilename, expected, expectRaise", [
    ('betl_test_google_api_key.json', 'betl_test_google_api_key.json', False),
    ('', '', True),
    (None, '', True),
])
def test_Setup_setGoogleAPIKeyFilename(apiKeyFilename,
                                       expected,
                                       expectRaise):
    setup = Setup()
    if expectRaise:
        with pytest.raises(ValueError):
            setup.setGoogleAPIKeyFilename(apiKeyFilename)
    else:
        setup.setGoogleAPIKeyFilename(apiKeyFilename)
        assert setup.GOOGLE_API_KEY_FILENAME == expected


@pytest.mark.parametrize("googleAccount, expected, expectRaise", [
    ('brian.spurling@gmail.com', 'brian.spurling@gmail.com', False),
    ('', '', True),
    (None, '', True),
])
def test_Setup_setGoogleAccount(googleAccount,
                                expected,
                                expectRaise):

    setup = Setup()
    if expectRaise:
        with pytest.raises(ValueError):
            setup.setGoogleAccount(googleAccount)
    else:
        setup.setGoogleAccount(googleAccount)
        assert setup.GOOGLE_ACCOUNT == expected


@pytest.mark.parametrize("adminPostgresUsername, expected, expectRaise", [
    ('b_spurling', 'b_spurling', False),
    ('', '', True),
    (None, '', True),
])
def test_Setup_setAdminPostgresUsername(adminPostgresUsername,
                                        expected,
                                        expectRaise):
    setup = Setup()
    if expectRaise:
        with pytest.raises(ValueError):
            setup.setAdminPostgresUsername(adminPostgresUsername)
    else:
        setup.setAdminPostgresUsername(adminPostgresUsername)
        assert setup.ADMIN_POSTGRES_USERNAME == expected


@pytest.mark.parametrize("adminPostgresPassword, expected", [
    ('test_value_01', 'test_value_01'),
    ('', ''),
    (None, '')])
def test_Setup_setAdminPostgresPassword(adminPostgresPassword, expected):
    setup = Setup()
    setup.setAdminPostgresPassword(adminPostgresPassword)
    assert setup.ADMIN_POSTGRES_PASSWORD == expected


@pytest.mark.parametrize("appRootPath, expected", [
    ('test_value_02', 'test_value_02'),
    ('', '.'),
    (None, '.')])
def test_Setup_setAppRootPath(appRootPath, expected):
    setup = Setup()
    setup.setAppRootPath(appRootPath)
    assert setup.APP_ROOT_PATH == expected


@pytest.mark.parametrize("tmpDataPath, expected", [
    ('test_value_03', './test_value_03'),
    ('', './tmp_data'),
    (None, './tmp_data')])
def test_Setup_setTmpDataPath(tmpDataPath, expected):
    setup = Setup()
    setup.setTmpDataPath(tmpDataPath)
    assert setup.TMP_DATA_PATH == expected


@pytest.mark.parametrize("srcDataPath, expected", [
    ('test_value_04', './test_value_04'),
    ('', './src_data'),
    (None, './src_data')])
def test_Setup_setSrcDataPath(srcDataPath, expected):
    setup = Setup()
    setup.setSrcDataPath(srcDataPath)
    assert setup.SRC_DATA_PATH == expected


@pytest.mark.parametrize("reportsPath, expected", [
    ('test_value_05', './test_value_05'),
    ('', './reports'),
    (None, './reports')])
def test_Setup_setReportsPath(reportsPath, expected):
    setup = Setup()
    setup.setReportsPath(reportsPath)
    assert setup.REPORTS_PATH == expected


@pytest.mark.parametrize("logsPath, expected", [
    ('test_value_06', './test_value_06'),
    ('', './logs'),
    (None, './logs')])
def test_Setup_setLogsPath(logsPath, expected):
    setup = Setup()
    setup.setLogsPath(logsPath)
    assert setup.LOG_PATH == expected


@pytest.mark.parametrize("schemaPath, expected", [
    ('test_value_07', './test_value_07'),
    ('', './schema'),
    (None, './schema')])
def test_Setup_setSchemaPath(schemaPath, expected):
    setup = Setup()
    setup.setSchemaPath(schemaPath)
    assert setup.SCHEMA_PATH == expected


@pytest.mark.parametrize("ctlDBHostName, expected", [
    ('test_value_08', 'test_value_08'),
    ('', 'localhost'),
    (None, 'localhost')])
def test_Setup_setCtlDBHostName(ctlDBHostName, expected):
    setup = Setup()
    setup.setCtlDBHostName(ctlDBHostName)
    assert setup.CTL_DB_HOST_NAME == expected


@pytest.mark.parametrize("ctlDBName, expected", [
    ('test_value_09', 'test_value_09'),
    ('', 'dwh_ctl'),
    (None, 'dwh_ctl')])
def test_Setup_setCtlDBName(ctlDBName, expected):
    setup = Setup()
    setup.setCtlDBName(ctlDBName)
    assert setup.CTL_DB_NAME == expected


@pytest.mark.parametrize("ctlDBUsername, expected, runAdminUNFuncFirst, adminPostgresUsername, expectRaise", [
    ('test_value_10', 'test_value_10', False, None, False),
    ('', '', False, None, True),
    (None, '', False, None, True),
    ('', 'test_value_10', True, 'test_value_10', False),
    (None, 'test_value_10', True, 'test_value_10', False)])
def test_Setup_setCtlDBUsername(ctlDBUsername,
                                expected,
                                runAdminUNFuncFirst,
                                adminPostgresUsername,
                                expectRaise):
    setup = Setup()
    if runAdminUNFuncFirst:
        setup.setAdminPostgresUsername(adminPostgresUsername)
    if expectRaise:
        with pytest.raises(ValueError):
            setup.setCtlDBUsername(ctlDBUsername)
    else:
        setup.setCtlDBUsername(ctlDBUsername)
        assert setup.CTL_DB_USERNAME == expected


@pytest.mark.parametrize("ctlDBPassword, expected", [
    ('test_value_11', 'test_value_11'),
    ('', ''),
    (None, '')])
def test_Setup_setCtlDBPassword(ctlDBPassword, expected):
    setup = Setup()
    setup.setCtlDBPassword(ctlDBPassword)
    assert setup.CTL_DB_PASSWORD == expected


@pytest.mark.parametrize("etlGSheetTitle, expected", [
    ('test_value_12', 'test_value_12'),
    ('', 'DWH - ETL DB SCHEMA'),
    (None, 'DWH - ETL DB SCHEMA')])
def test_Setup_setSchemaDescETLGsheetTitle(etlGSheetTitle, expected):
    setup = Setup()
    setup.setSchemaDescETLGsheetTitle(etlGSheetTitle)
    assert setup.SCHEMA_DESC_ETL_GSHEET_TITLE == expected


@pytest.mark.parametrize("trgGSheetTitle, expected", [
    ('test_value_13', 'test_value_13'),
    ('', 'DWH - TRG DB SCHEMA'),
    (None, 'DWH - TRG DB SCHEMA')])
def test_Setup_setSchemaDescTRGGsheetTitle(trgGSheetTitle, expected):
    setup = Setup()
    setup.setSchemaDescTRGGsheetTitle(trgGSheetTitle)
    assert setup.SCHEMA_DESC_TRG_GSHEET_TITLE == expected


@pytest.mark.parametrize("etlDBHostName, expected", [
    ('test_value_14', 'test_value_14'),
    ('', 'localhost'),
    (None, 'localhost')])
def test_Setup_setETLDBHostName(etlDBHostName, expected):
    setup = Setup()
    setup.setETLDBHostName(etlDBHostName)
    assert setup.ETL_DB_HOST_NAME == expected


@pytest.mark.parametrize("etlDBName, expected", [
    ('test_value_15', 'test_value_15'),
    ('', 'dwh_etl'),
    (None, 'dwh_etl')])
def test_Setup_setETLDBName(etlDBName, expected):
    setup = Setup()
    setup.setETLDBName(etlDBName)
    assert setup.ETL_DB_NAME == expected


@pytest.mark.parametrize("etlDBUsername, expected, runAdminUNFuncFirst, adminPostgresUsername, expectRaise", [
    ('test_value_16', 'test_value_16', False, None, False),
    ('', '', False, None, True),
    (None, '', False, None, True),
    ('', 'test_value_16', True, 'test_value_16', False),
    (None, 'test_value_16', True, 'test_value_16', False)])
def test_Setup_setETLDBUsername(etlDBUsername,
                                expected,
                                runAdminUNFuncFirst,
                                adminPostgresUsername,
                                expectRaise):
    setup = Setup()
    if runAdminUNFuncFirst:
        setup.setAdminPostgresUsername(adminPostgresUsername)
    if expectRaise:
        with pytest.raises(ValueError):
            setup.setETLDBUsername(etlDBUsername)
    else:
        setup.setETLDBUsername(etlDBUsername)
        assert setup.ETL_DB_USERNAME == expected


@pytest.mark.parametrize("etlDBPassword, expected", [
    ('test_value_17', 'test_value_17'),
    ('', ''),
    (None, '')])
def test_Setup_setETLDBPassword(etlDBPassword, expected):
    setup = Setup()
    setup.setETLDBPassword(etlDBPassword)
    assert setup.ETL_DB_PASSWORD == expected


@pytest.mark.parametrize("trgDBHostName, expected", [
    ('test_value_18', 'test_value_18'),
    ('', 'localhost'),
    (None, 'localhost')])
def test_Setup_setTRGDBHostName(trgDBHostName, expected):
    setup = Setup()
    setup.setTRGDBHostName(trgDBHostName)
    assert setup.TRG_DB_HOST_NAME == expected


@pytest.mark.parametrize("trgDBName, expected", [
    ('test_value_19', 'test_value_19'),
    ('', 'dwh_trg'),
    (None, 'dwh_trg')])
def test_Setup_setTRGDBName(trgDBName, expected):
    setup = Setup()
    setup.setTRGDBName(trgDBName)
    assert setup.TRG_DB_NAME == expected


@pytest.mark.parametrize("trgDBUsername, expected, runAdminUNFuncFirst, adminPostgresUsername, expectRaise", [
    ('test_value_20', 'test_value_20', False, None, False),
    ('', '', False, None, True),
    (None, '', False, None, True),
    ('', 'test_value_20', True, 'test_value_20', False),
    (None, 'test_value_20', True, 'test_value_20', False)])
def test_Setup_setTRGDBUsername(trgDBUsername,
                                expected,
                                runAdminUNFuncFirst,
                                adminPostgresUsername,
                                expectRaise):
    setup = Setup()
    if runAdminUNFuncFirst:
        setup.setAdminPostgresUsername(adminPostgresUsername)
    if expectRaise:
        with pytest.raises(ValueError):
            setup.setTRGDBUsername(trgDBUsername)
    else:
        setup.setTRGDBUsername(trgDBUsername)
        assert setup.TRG_DB_USERNAME == expected


@pytest.mark.parametrize("trgDBPassword, expected", [
    ('test_value_21', 'test_value_21'),
    ('', ''),
    (None, '')])
def test_Setup_setTRGDBPassword(trgDBPassword, expected):
    setup = Setup()
    setup.setTRGDBPassword(trgDBPassword)
    assert setup.TRG_DB_PASSWORD == expected


@pytest.mark.parametrize("defaultRowsGSheetTitle, expected", [
    ('test_value_22', 'test_value_22'),
    ('', 'DWH - Default Rows'),
    (None, 'DWH - Default Rows')])
def test_Setup_setDefaultRowsGSheetTitle(defaultRowsGSheetTitle, expected):
    setup = Setup()
    setup.setDefaultRowsGSheetTitle(defaultRowsGSheetTitle)
    assert setup.DEFAULT_ROWS_GHSEET_TITLE == expected


@pytest.mark.parametrize("mdmGSheetTitle, expected", [
    ('test_value_23', 'test_value_23'),
    ('', 'DWH - Master Data Mappings'),
    (None, 'DWH - Master Data Mappings')])
def test_Setup_setMDMGSheetTitle(mdmGSheetTitle, expected):
    setup = Setup()
    setup.setMDMGSheetTitle(mdmGSheetTitle)
    assert setup.MDM_GHSEET_TITLE == expected


# Override the Python built-in input method
# SetupClass.input = lambda x: apiKeyFilename
# def teardown_method(self, method):
#     # This method is being called after each test case, and it
#     # will revert input back to original function
#     Setup.input = input
