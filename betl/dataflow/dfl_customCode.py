from betl.io import dbIO


def customSQL(self, sql, desc, databaseID, dataset=None):

    self.stepStart(desc=desc, additionalDesc=sql)

    datastore = self.CONF.DATA.getDWHDatastore(databaseID)

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


def applyFunctionToColumns(self,
                           dataset,
                           function,
                           columns,
                           desc,
                           targetColumns=None):

    self.stepStart(desc=desc)

    if isinstance(columns, str):
        columns = [columns]

    if isinstance(targetColumns, str):
        targetColumns = [targetColumns]

    cnt = 0
    cleanedColName = ''
    for column in columns:
        if targetColumns is None:
            cleanedColName = column
        else:
            cleanedColName = targetColumns[cnt]
        cnt += 1

        self.data[dataset][cleanedColName] = \
            function(self.data[dataset][column])

    self.stepEnd(
        report='',
        datasetName=dataset,
        df=self.data[dataset])


def applyFunctionToRows(self,
                        dataset,
                        function,
                        desc):

    self.stepStart(desc=desc)

    for row in self.data[dataset].itertuples():
        function(row)

    report = ''

    self.stepEnd(report=report)
