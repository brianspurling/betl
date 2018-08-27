databases = ['ETL', 'TRG']

dataLayers = {'EXT': 'ETL',
              'TRN': 'ETL',
              'LOD': 'ETL',
              'BSE': 'TRG',
              'SUM': 'TRG'}

auditColumns = {
    'colNames': [
        'audit_source_system',
        'audit_bulk_load_date',
        'audit_latest_delta_load_date',
        'audit_latest_load_operation'
    ],
    'dataType': [
        'TEXT',
        'DATE',
        'DATE',
        'TEXT'
    ],
}

defaultScheduleConfig = {
    'DEFAULT_EXTRACT': False,
    'EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],
    'DEFAULT_LOAD': False,
    'DEFAULT_SUMMARISE': False,
    'DEFAULT_DM_DATE': False,
    'BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],
    'EXTRACT_DATAFLOWS': [],
    'TRANSFORM_DATAFLOWS': [],
    'LOAD_DATAFLOWS': [],
    'SUMMARISE_DATAFLOWS': []
}
