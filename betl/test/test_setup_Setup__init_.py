from betl.setupModule import Setup


# These values are the default values for Setup(), which all get set
# in the init function
def test_Setup_init():

    setup = Setup()

    assert setup.GOOGLE_API_SCOPE == [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive']

    assert setup.DWH_ID == 'DWH'

    # These three must be set by the user - no default value possible
    assert setup.GOOGLE_API_KEY_FILENAME is None
    assert setup.GOOGLE_ACCOUNT is None
    assert setup.ADMIN_POSTGRES_USERNAME is None

    assert setup.ADMIN_POSTGRES_PASSWORD == ''

    assert setup.APP_ROOT_PATH == '.'
    assert setup.TMP_DATA_PATH == './tmp_data'
    assert setup.SRC_DATA_PATH == './src_data'
    assert setup.REPORTS_PATH == './reports'
    assert setup.LOG_PATH == './logs'
    assert setup.SCHEMA_PATH == './schema'

    assert setup.CTL_DB_HOST_NAME == 'localhost'
    assert setup.CTL_DB_NAME == 'dwh_ctl'
    assert setup.CTL_DB_USERNAME is None
    assert setup.CTL_DB_PASSWORD == ''

    assert setup.SCHEMA_DESC_ETL_GSHEET_TITLE == 'DWH - ETL DB SCHEMA'
    assert setup.SCHEMA_DESC_TRG_GSHEET_TITLE == 'DWH - TRG DB SCHEMA'

    assert setup.ETL_DB_HOST_NAME == 'localhost'
    assert setup.ETL_DB_NAME == 'dwh_etl'
    assert setup.ETL_DB_USERNAME is None
    assert setup.ETL_DB_PASSWORD == ''

    assert setup.TRG_DB_HOST_NAME == 'localhost'
    assert setup.TRG_DB_NAME == 'dwh_trg'
    assert setup.TRG_DB_USERNAME is None
    assert setup.TRG_DB_PASSWORD == ''

    assert setup.DEFAULT_ROWS_GHSEET_TITLE == 'DWH - Default Rows'
    assert setup.MDM_GHSEET_TITLE == 'DWH - Master Data Mappings'

    # Let's not allow any other attributes to be added to the setup class
    # without causing a test failure. This should ensure we add
    assert list(setup.__dict__.keys()) == [
        'GOOGLE_API_SCOPE',
        'DWH_ID',
        'GOOGLE_API_KEY_FILENAME',
        'GOOGLE_ACCOUNT',
        'ADMIN_POSTGRES_USERNAME',
        'ADMIN_POSTGRES_PASSWORD',
        'APP_ROOT_PATH',
        'TMP_DATA_PATH',
        'SRC_DATA_PATH',
        'REPORTS_PATH',
        'LOG_PATH',
        'SCHEMA_PATH',
        'CTL_DB_HOST_NAME',
        'CTL_DB_NAME',
        'CTL_DB_USERNAME',
        'CTL_DB_PASSWORD',
        'SCHEMA_DESC_ETL_GSHEET_TITLE',
        'SCHEMA_DESC_TRG_GSHEET_TITLE',
        'ETL_DB_HOST_NAME',
        'ETL_DB_NAME',
        'ETL_DB_USERNAME',
        'ETL_DB_PASSWORD',
        'TRG_DB_HOST_NAME',
        'TRG_DB_NAME',
        'TRG_DB_USERNAME',
        'TRG_DB_PASSWORD',
        'DEFAULT_ROWS_GHSEET_TITLE',
        'MDM_GHSEET_TITLE'], "If you add a new attribute to the setup class you need to add the appropriate set functions and tests"
