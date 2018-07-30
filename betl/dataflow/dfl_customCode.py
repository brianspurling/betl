from betl.io import dbIO


def customSQL(self, sql, desc, dataLayer, dataset=None):

    self.stepStart(desc=desc, additionalDesc=sql)

    datastore = \
        self.conf.DATA.getDataLayerLogicalSchema(dataLayer).datastore

    if dataset is not None:
        self.data[dataset] = dbIO.customSQL(sql, datastore)
    else:
        dbIO.customSQL(sql, datastore)

    report = ''

    if dataset is not None:
        self.stepEnd(
            report=report,
            datasetName=dataset,
            df=self.data[dataset])
    else:
        self.stepEnd(
            report=report,
            datasetName=dataset)


def iterate(self, dataset, function, desc):

    self.stepStart(desc=desc)

    for row in self.data[dataset].itertuples():
        function(row)

    report = ''

    self.stepEnd(report=report)


def cleanColumn(self,
                dataset,
                cleaningFunc,
                columns,
                desc,
                cleanedColumns=None):

    self.stepStart(desc=desc)

    cnt = 0
    cleanedColName = ''
    for column in columns:
        if cleanedColumns is None:
            cleanedColName = column
        else:
            cleanedColName = cleanedColumns[cnt]
        cnt += 1

        self.data[dataset][cleanedColName] = \
            cleaningFunc(self.data[dataset][column])

    self.stepEnd(
        report='',
        datasetName=dataset,
        df=self.data[dataset])
