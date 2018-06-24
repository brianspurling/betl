databases = ['ETL', 'TRG']

dataLayers = ['SRC', 'STG', 'TRG', 'SUM']

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
    'SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],
    'DEFAULT_LOAD': False,
    'DEFAULT_SUMMARISE': False,
    'DEFAULT_DM_DATE': False,
    'TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],
    'EXTRACT_DATAFLOWS': [],
    'TRANSFORM_DATAFLOWS': [],
    'LOAD_DATAFLOWS': [],
    'SUMMARISE_DATAFLOWS': []
}
