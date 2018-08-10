import pytest
import pandas as pd

from betl.dataflow import DataFlow


@pytest.fixture
def dataset():
    return pd.DataFrame(
        {'col1': ['1', '2']})


def concatX(row):
    return row['col1'] + 'x'


@pytest.mark.parametrize("columns, expected", [
    ({'col2': 'test2',
      'col3': 'test3'}, {'col1': ['1', '2'],
                         'col2': ['test2', 'test2'],
                         'col3': ['test3', 'test3']}),
    ({'col2': ['x', 'y']}, {'col1': ['1', '2'],
                            'col2': ['x', 'y']}),
    ({'col2': concatX}, {'col1': ['1', '2'],
                         'col2': ['1x', '2x']})])
def test_addColumns(conf,
                    dataset,
                    columns,
                    expected):

    dfl = DataFlow(
        desc='test dataflow',
        conf=conf,
        recordInCtrlDB=False)

    dfl.createDataset(
        dataset='testDataset',
        data=dataset,
        desc='Create test dataset')

    dfl.addColumns(
        dataset='testDataset',
        columns=columns,
        desc='Drop columns')

    df_expected = pd.DataFrame(expected)

    assert dfl.data['testDataset'].equals(df_expected)
