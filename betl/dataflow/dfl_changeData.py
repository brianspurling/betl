import pandas as pd


def setNulls(self, dataset, columns, desc):

    self.stepStart(desc=desc)

    for col in columns:
        self.data[dataset].loc[self.data[dataset][col].isnull(), col] = \
            columns[col]

    report = ''

    self.stepEnd(report=report)


def toNumeric(self,
              dataset,
              columns,
              desc,
              cleanedColumns=None,
              cast=None):

    self.stepStart(desc=desc)

    if isinstance(columns, str):
        columns = [columns]

    cnt = 0
    for column in columns:

        if cleanedColumns is None:
            cleanedColumn = column
        else:
            cleanedColumn = cleanedColumns[cnt]
        cnt += 1

        self.data[dataset][cleanedColumn] = \
            self.data[dataset][column].str.replace(r"[^\d.]+", '')

        self.data[dataset][cleanedColumn] = \
            pd.to_numeric(
                arg=self.data[dataset][cleanedColumn],
                errors='coerce')

        if cast == 'integer':
            self.data[dataset][cleanedColumn] = \
                self.data[dataset][cleanedColumn].fillna(0).astype(int)
        else:
            raise ValueError('You tried to cast to a type not yet ' +
                             'handled by dataflow.toNumeric: ' + cast)

        # TODO need to pick up look here
        report = 'Cleaned ' + column + ' to ' + cleanedColumn

    self.stepEnd(
        report=report,
        datasetName=dataset,
        df=self.data[dataset])


def replace(self,
            dataset,
            columnNames,
            toReplace,
            value,
            desc,
            regex=False):

    self.stepStart(desc=desc)

    if columnNames is not None:

        if isinstance(columnNames, str):
            columnNames = [columnNames]

        for columnName in columnNames:
            self.data[dataset][columnName].replace(
                    to_replace=toReplace,
                    value=value,
                    regex=regex,
                    inplace=True)
    else:
        self.data[dataset].replace(
                to_replace=toReplace,
                value=value,
                regex=regex,
                inplace=True)

    # TODO: report
    report = ''

    self.stepEnd(
        report=report,
        datasetName=dataset,  # optional
        df=self.data[dataset],  # optional
        shapeOnly=False)  # optional


def setColumns(self, dataset, columns, desc):
    # A wrapper for semantic purposes
    self.addColumns(dataset, columns, desc)
