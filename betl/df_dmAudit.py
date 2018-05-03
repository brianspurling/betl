from . import api as betl


def getSchemaDescription():

    # This schema description reflects the same meta data structure that
    # we find in the schema spreadsheets.
    tableSchema = {
        'tableName': 'dm_audit',
        'columnSchemas': {}
    }

    tableSchema['columnSchemas']['audit_id'] = {
        'tableName':   'dm_audit',
        'columnName':  'audit_id',
        'dataType':    'TEXT',
        'columnType':  'Surrogate key',
        'fkDimension': None
    }

    tableSchema['columnSchemas']['latest_delta_load_operation'] = {
        'tableName':   'dm_audit',
        'columnName':  'latest_delta_load_operation',
        'dataType':    'TEXT',
        'columnType':  'Natural Key',
        'fkDimension': None
    }

    tableSchema['columnSchemas']['data_quality_score'] = {
        'tableName':   'dm_audit',
        'columnName':  'data_quality_score',
        'dataType':    'INTEGER',
        'columnType':  'Natural Key',
        'fkDimension': None
    }

    return tableSchema


def transformDMAudit(scheduler):

    dfl = betl.DataFlow(desc='Generate the dm_audit rows')

    dfl.createDataset(
        dataset='ops',
        data={'latest_delta_load_operation': ['INSERT', 'UPDATE', 'DELETE'],
              'temp_key': 1},
        desc='A row for insert, update and delete operations')

    dfl.createDataset(
        dataset='dq_scores',
        data={'data_quality_score': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
              'temp_key': 1},
        desc='A row for each DQ score')

    dfl.join(
        datasets=['ops', 'dq_scores'],
        targetDataset='trg_dm_audit',
        joinCol='temp_key',
        how='left',
        keepCols=['latest_delta_load_operation', 'data_quality_score'],
        desc='Cartesian join the two datasets together on the temp_key')

    dfl.write(
        dataset='trg_dm_audit',
        targetTableName='trg_dm_audit',
        dataLayerID='STG')
