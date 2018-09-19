from betl.test import test


def prepForLoad(self,
                dataset,
                naturalKeyCols=None,
                targetTableName=None,
                keepDataflowOpen=False,
                desc=None):

    if targetTableName is None:
        targetTableName = dataset

    if naturalKeyCols is not None:
        self.collapseNaturalKeyCols(
            dataset=dataset,
            targetTableName=targetTableName,
            naturalKeyCols=naturalKeyCols)

    self.write(
        dataset=dataset,
        targetTableName=targetTableName,
        dataLayerID='LOD',
        keepDataflowOpen=keepDataflowOpen,
        forceDBWrite=False,  # We never write the LOD layer to db
        desc=desc)


def collapseNaturalKeyCols(self,
                           dataset,
                           targetTableName,
                           naturalKeyCols):

    desc = 'Collapsing natural keys into a single column on dataset ' + \
           targetTableName + ', ready for ' + 'NK/SK lookup. Columns: ' + \
           str(list(naturalKeyCols))

    self.stepStart(desc=desc)

    for nkCol in naturalKeyCols:

        # Create the NK column empty, then concat the other cols on one by one
        self.data[dataset][nkCol] = ''

        srcCols = naturalKeyCols[nkCol]
        if isinstance(srcCols, str):
            srcCols = [srcCols]

        i = 1
        for srcCol in srcCols:
            separator = '_'
            if i == len(srcCols):
                separator = ''
            i += 1

            self.data[dataset][nkCol] = \
                self.data[dataset][nkCol] + self.data[dataset][srcCol] + \
                separator

            self.data[dataset].drop(
                srcCol,
                axis=1,
                inplace=True)

    report = 'Collapsed ' + str(len(naturalKeyCols)) + ' NK cols'

    self.stepEnd(
        report=report,
        datasetName=dataset,
        df=self.data[dataset])
