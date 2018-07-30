import pandas as pd
import pprint
from betl.logger import logger


def join(self,
         datasets,
         targetDataset,
         how,
         desc,
         joinCol=None,
         leftJoinCol=None,
         rightJoinCol=None,
         keepCols=None):

    self.stepStart(desc=desc)

    if len(datasets) > 2:
        raise ValueError('You can only join two tables at once')

    self.data[targetDataset] = pd.merge(
        self.data[datasets[0]],
        self.data[datasets[1]],
        on=joinCol,
        left_on=leftJoinCol,
        right_on=rightJoinCol,
        how=how)
    if keepCols is not None:
        self.data[targetDataset] = self.data[targetDataset][keepCols]

    report = 'Joined datasets ' + datasets[0] + ' ('
    report += str(self.data[datasets[0]].shape[0]) + ' rows) and '
    report += datasets[1] + ' ('
    report += str(self.data[datasets[1]].shape[0]) + ' rows) into '
    report += targetDataset + ' ('
    report += str(self.data[targetDataset].shape[0]) + ' rows)'

    self.stepEnd(
        report=report,
        datasetName=targetDataset,
        df=self.data[targetDataset])


def union(self, datasets, targetDataset, desc):

    self.stepStart(desc=desc)

    try:
        self.data[targetDataset] = \
            pd.concat([self.data[dataset] for dataset in datasets])
    except AssertionError:
        error = ''
        for dataset in datasets:
            error += '** ' + dataset + ' (sorted) **\n'
            error += '\n'
            error += pprint.pformat(sorted(list(
                self.data[dataset].columns)))
            error += '\n'
            error += '\n'
        logger.logStepError(error)
        raise

    report = 'Concatenated ' + str(len(datasets)) + \
             ' dfs, totalling ' + \
             str(self.data[targetDataset].shape[0]) + ' rows'

    self.stepEnd(
        report=report,
        datasetName=targetDataset,
        df=self.data[targetDataset])
