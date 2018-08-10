import pytest
import pandas as pd

from betl.dataflow import DataFlow
from betl.test import conftest


@pytest.fixture
def dataset_renameColumns():
    return pd.DataFrame(
        {'col1': ['1', '2'],
         'col2': ['a', 'b']})


def test_renameColumns(conf, dataset_renameColumns):

    dfl = DataFlow(
        desc='test_renameColumns',
        conf=conf,
        recordInCtrlDB=False)

    dfl.createDataset(
        dataset='testDataset',
        data=dataset_renameColumns,
        desc='Create test dataset')

    dfl.renameColumns(
        dataset='testDataset',
        columns={'col1': 'renamedCol1'},
        desc='Test column rename')

    assert dfl.getColumnList(dataset='testDataset') == ['renamedCol1', 'col2']
