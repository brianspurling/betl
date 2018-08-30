import pytest
import pandas as pd

from betl.dataflow import DataFlow


@pytest.fixture
def dataset():
    return pd.DataFrame(
        {'col1': ['1', '2'],
         'col2': ['a', 'b'],
         'col3': ['x', 'y']})


@pytest.mark.parametrize("dropAudit, expected", [
    (True, ['col2', 'col3']),
    (False, ['col2',
             'col3',
             'audit_source_system',
             'audit_bulk_load_date',
             'audit_latest_delta_load_date',
             'audit_latest_load_operation'])])
def test_dropColumns_dropSingle(conf,
                                dataset,
                                dropAudit,
                                expected):

    dfl = DataFlow(
        desc='test dataflow',
        conf=conf,
        recordInCtrlDB=False)

    dfl.createDataset(
        dataset='testDataset',
        data=dataset,
        desc='Create test dataset')

    dfl.setAuditCols(
        dataset='testDataset',
        bulkOrDelta='bulk',
        sourceSystem='TST',
        desc='Add audit cols')

    dfl.dropColumns(
        dataset='testDataset',
        colsToDrop='col1',
        desc='Drop columns',
        dropAuditCols=dropAudit)

    colList = dfl.getColumnList(
        dataset='testDataset',
        desc='get col list')

    assert colList == expected


@pytest.mark.parametrize("dropAudit, expected", [
    (True, ['col1']),
    (False, ['col1',
             'audit_source_system',
             'audit_bulk_load_date',
             'audit_latest_delta_load_date',
             'audit_latest_load_operation'])])
def test_dropColumns_dropMultiple(conf,
                                  dataset,
                                  dropAudit,
                                  expected):

    dfl = DataFlow(
        desc='test dataflow',
        conf=conf,
        recordInCtrlDB=False)

    dfl.createDataset(
        dataset='testDataset',
        data=dataset,
        desc='Create test dataset')

    dfl.setAuditCols(
        dataset='testDataset',
        bulkOrDelta='bulk',
        sourceSystem='TST',
        desc='Add audit cols')

    dfl.dropColumns(
        dataset='testDataset',
        colsToDrop=['col2', 'col3'],
        desc='Drop columns',
        dropAuditCols=dropAudit)

    colList = dfl.getColumnList(
        dataset='testDataset',
        desc='get col list')

    assert colList == expected


@pytest.mark.parametrize("dropAudit, expected", [
    (True, ['col3']),
    (False, ['col3',
             'audit_source_system',
             'audit_bulk_load_date',
             'audit_latest_delta_load_date',
             'audit_latest_load_operation'])])
def test_dropColumns_keepSingle(conf,
                                dataset,
                                dropAudit,
                                expected):

    dfl = DataFlow(
        desc='test dataflow',
        conf=conf,
        recordInCtrlDB=False)

    dfl.createDataset(
        dataset='testDataset',
        data=dataset,
        desc='Create test dataset')

    dfl.setAuditCols(
        dataset='testDataset',
        bulkOrDelta='bulk',
        sourceSystem='TST',
        desc='Add audit cols')

    dfl.dropColumns(
        dataset='testDataset',
        colsToKeep='col3',
        desc='Drop columns',
        dropAuditCols=dropAudit)

    colList = dfl.getColumnList(
        dataset='testDataset',
        desc='get col list')

    assert colList == expected


@pytest.mark.parametrize("dropAudit, expected", [
    (True, ['col2', 'col3']),
    (False, ['col2',
             'col3',
             'audit_source_system',
             'audit_bulk_load_date',
             'audit_latest_delta_load_date',
             'audit_latest_load_operation'])])
def test_dropColumns_keepMultiple(conf,
                                  dataset,
                                  dropAudit,
                                  expected):

    dfl = DataFlow(
        desc='test dataflow',
        conf=conf,
        recordInCtrlDB=False)

    dfl.createDataset(
        dataset='testDataset',
        data=dataset,
        desc='Create test dataset')

    dfl.setAuditCols(
        dataset='testDataset',
        bulkOrDelta='bulk',
        sourceSystem='TST',
        desc='Add audit cols')

    dfl.dropColumns(
        dataset='testDataset',
        colsToKeep=['col2', 'col3'],
        desc='Drop columns',
        dropAuditCols=dropAudit)

    colList = dfl.getColumnList(
        dataset='testDataset',
        desc='get col list')

    assert colList == expected
