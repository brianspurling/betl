import pytest
import pandas as pd

from betl.dataflow import DataFlow


@pytest.fixture
def dataset():
    return pd.DataFrame(
        {'A': ['a', 'b', 'c'],
         'B': ['1', '3', '5'],
         'C': ['2', '4', '6']})


dataset_pivoted1 = pd.DataFrame(
    {'A': ['a', 'a', 'b', 'b', 'c', 'c'],
     'newVarCol': ['B', 'C', 'B', 'C', 'B', 'C'],
     'newValCol': ['1', '2', '3', '4', '5', '6']})


dataset_pivoted2 = pd.DataFrame(
    {'A': ['a', 'b', 'c'],
     'B': ['1', '3', '5'],
     'newVarCol': ['C', 'C', 'C'],
     'newValCol': ['2', '4', '6']})


@pytest.mark.parametrize("colsNotToPivot," +
                         "colsToPivot," +
                         "varName," +
                         "valueName, " +
                         "expected", [
    (['A'], ['B', 'C'], 'newVarCol', 'newValCol', dataset_pivoted1),
    (['A', 'B'], ['C'], 'newVarCol', 'newValCol', dataset_pivoted2)])
def test_pivotColsToRows(conf,
                         dataset,
                         colsNotToPivot,
                         colsToPivot,
                         varName,
                         valueName,
                         expected):

    dfl = DataFlow(
        desc='test dataflow',
        conf=conf,
        recordInCtrlDB=False)

    dfl.createDataset(
        dataset='testDataset',
        data=dataset,
        desc='Create test dataset')

    dfl.pivotColsToRows(
        dataset='testDataset',
        colsNotToPivot=colsNotToPivot,
        colsToPivot=colsToPivot,
        varName=varName,
        valueName=valueName,
        desc='Pivot columns')

    dfl.dropColumns(
        dataset='testDataset',
        colsToDrop=None,
        desc='Drop columns',
        dropAuditCols=True)

    # Need to sort columns, values, and reset indexes before comparing dataframes
    expected.sort_index(axis=1, inplace=True)
    expected.sort_values(list(expected), axis=0, inplace=True)
    expected.reset_index(drop=True, inplace=True)
    dfl.data['testDataset'].sort_index(axis=1, inplace=True)
    dfl.data['testDataset'].sort_values(list(dfl.data['testDataset']), axis=0, inplace=True)
    dfl.data['testDataset'].reset_index(drop=True, inplace=True)

    assert dfl.data['testDataset'].equals(expected)
