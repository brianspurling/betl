from betl.io import fileIO
from betl.io import dbIO
import numpy as np


def truncate(self, dataset, dataLayerID, desc, forceDBWrite=False):
    self.stepStart(desc=desc)

    path = (self.CONF.TMP_DATA_PATH + '/' + dataLayerID + '/')
    filename = dataset + '.csv'

    fileIO.truncateFile(self.CONF, path, filename)

    if forceDBWrite:
        dataLayer = self.CONF.getLogicalSchemaDataLayer(dataLayerID)
        dbIO.truncateTable(dataset, dataLayer.getDatastore(), dataLayer.schema)

    report = ''

    self.stepEnd(report=report)


def dedupe(self, dataset, desc):

    self.stepStart(desc=desc)

    self.data[dataset].drop_duplicates(inplace=True)

    report = ''

    self.stepEnd(
        report=report,
        datasetName=dataset,
        df=self.data[dataset])


def filter(self, dataset, filters, desc, targetDataset=None):

    self.stepStart(desc=desc)

    _targetDataset = dataset
    if targetDataset is not None:
        _targetDataset = targetDataset

    originalLength = self.data[dataset].shape[0]

    for f in filters:
        if isinstance(filters[f], str):
            self.data[_targetDataset] = \
                self.data[dataset].loc[
                    self.data[dataset][f] == filters[f]]
        elif isinstance(filters[f], tuple):
            if filters[f][0] == '>':
                self.data[_targetDataset] = \
                    self.data[dataset].loc[
                        self.data[dataset][f] > filters[f][1]]
            elif filters[f][0] == '<':
                self.data[_targetDataset] = \
                    self.data[dataset].loc[
                        self.data[dataset][f] > filters[f][1]]
            elif filters[f][0] == '==':
                self.data[_targetDataset] = \
                    self.data[dataset].loc[
                        self.data[dataset][f] == filters[f][1]]
            elif filters[f][0] == '!=':
                self.data[_targetDataset] = \
                    self.data[dataset].loc[
                        self.data[dataset][f] != filters[f][1]]
            elif filters[f][0] == 'not in':
                self.data[_targetDataset] = \
                    self.data[dataset].loc[
                        ~self.data[dataset][f].isin(filters[f][1])]
            else:
                raise ValueError(
                    'Filter currently only support ==, !=, < or >')
        else:
            raise ValueError('filter value must be str or tuple (not ' +
                             str(type(filters[f])) + ')')

    newLength = self.data[_targetDataset].shape[0]
    if originalLength == 0:
        pcntChange = 0
    else:
        pcntChange = (originalLength - newLength) / originalLength
    pcntChange = round(pcntChange * 100, 1)
    report = 'Filtered dataset from ' + str(originalLength) + ' to ' + \
             str(newLength) + ' rows (' + str(pcntChange) + ')'

    self.stepEnd(
        report=report,
        datasetName=_targetDataset,
        df=self.data[_targetDataset],
        shapeOnly=True)


def filterWhereNotIn(self,
                     datasetToBeFiltered,
                     columnsToBeFiltered,
                     datasetToFilterBy,
                     columnsToFilterBy,
                     targetDataset,
                     desc):

    self.stepStart(desc=desc)

    origCols = list(self.data[datasetToBeFiltered])
    removeColumnToFilterBy = False
    removeColumnToBeFiltered = False

    if isinstance(columnsToBeFiltered, str):
        columnToBeFiltered = columnsToBeFiltered
    if isinstance(columnsToBeFiltered, list):
        if len(columnsToBeFiltered) == 1:
            columnToBeFiltered = columnsToBeFiltered[0]
        else:
            removeColumnToBeFiltered = True
            columnToBeFiltered = "".join(columnsToBeFiltered) + 'pwqnct'
            self.data[datasetToBeFiltered][columnToBeFiltered] = ''
            for col in columnsToBeFiltered:
                self.data[datasetToBeFiltered][columnToBeFiltered] = \
                    self.data[datasetToBeFiltered][columnToBeFiltered] + \
                    self.data[datasetToBeFiltered][col].map(str)

    if isinstance(columnsToFilterBy, str):
        columnToFilterBy = columnsToFilterBy
    if isinstance(columnsToFilterBy, list):
        if len(columnsToFilterBy) == 1:
            columnToFilterBy = columnsToFilterBy[0]
        else:
            removeColumnToFilterBy = True
            columnToFilterBy = "".join(columnsToFilterBy) + 'pwqnct'
            self.data[datasetToFilterBy][columnToFilterBy] = ''
            for col in columnsToFilterBy:
                self.data[datasetToFilterBy][columnToFilterBy] = \
                    self.data[datasetToFilterBy][columnToFilterBy] + \
                    self.data[datasetToFilterBy][col].map(str)

    self.data[targetDataset] = self.data[datasetToBeFiltered].loc[
        (np.logical_not(
            self.data[datasetToBeFiltered][columnToBeFiltered].isin(
                self.data[datasetToFilterBy][columnToFilterBy]))),
        origCols].copy()

    if removeColumnToBeFiltered:
        self.data[datasetToBeFiltered].drop(
            columnToBeFiltered,
            axis=1,
            inplace=True)

    if removeColumnToFilterBy:
        self.data[datasetToFilterBy].drop(
            columnToFilterBy,
            axis=1,
            inplace=True)

    report = ''

    self.stepEnd(
        report=report,
        datasetName=targetDataset,
        df=self.data[targetDataset],
        shapeOnly=False)
