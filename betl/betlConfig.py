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
