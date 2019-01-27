import pytest
import pandas as pd

from betl.dataflow import DataFlow


@pytest.fixture
def dataset():
    return pd.DataFrame(
        {'col1': ['1', '2'],
         'col2': ['a', 'b']})


def test_queueForLoad(conf, dataset):

    dfl = DataFlow(
        desc='test dataflow',
        conf=conf)

    dfl.createDataset(
        dataset='testDataset',
        data=dataset,
        desc='Create test dataset')

    dfl.prepForLoad(
        dataset='testDataset',
        targetTableName='dm_test_dimension',
        keepDataflowOpen=True)

    dfl.read(
        tableName='dm_test_dimension',
        dataLayer='LOD')

    assert dfl.getColumnList(dataset='dm_test_dimension') == ['col1', 'col2']
