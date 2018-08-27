import pandas as pd


def renameColumns(self, dataset, columns, desc, silent=False):

    self.stepStart(desc=desc, silent=silent)

    self.data[dataset].rename(index=str,
                              columns=columns,
                              inplace=True)

    report = 'Renamed ' + str(len(columns)) + ' columns'

    self.stepEnd(
        report=report,
        datasetName=dataset,
        df=self.data[dataset],
        silent=silent)


def dropColumns(self,
                dataset,
                colsToDrop=None,
                colsToKeep=None,
                desc=None,
                dropAuditCols=False,
                silent=False):

    if isinstance(colsToDrop, str):
        colsToDrop = [colsToDrop]
    if isinstance(colsToKeep, str):
        colsToKeep = [colsToKeep]

    if desc is None:
        desc = 'Dropping columns from ' + dataset + ': ' + str(colsToDrop)
    self.stepStart(desc=desc, silent=silent)

    if colsToDrop is not None and colsToKeep is not None:
        raise ValueError("Nope!")

    auditCols = self.CONF.DATA.AUDIT_COLS['colNames'].tolist()
    if colsToKeep is not None:
        colsToKeep = colsToKeep + auditCols
        colsToDrop = [col for col in list(self.data[dataset])
                      if col not in colsToKeep]

    if dropAuditCols and set(auditCols).issubset(list(self.data[dataset])):
        if colsToDrop is None:
            colsToDrop = []
        colsToDrop += auditCols

    self.data[dataset].drop(
        colsToDrop,
        axis=1,
        inplace=True)

    report = 'Dropped ' + str(len(colsToDrop)) + ' columns'

    self.stepEnd(
        report=report,
        datasetName=dataset,
        df=self.data[dataset],
        silent=silent)


def addColumns(self, dataset, columns, desc):

    self.stepStart(desc=desc)

    # columns is a dictionary of columnName:value pairs
    # The value can be a hard-coded value or a series. Both will work
    # with a simple pandas assigment. The value can also be a function.
    # If so, it needs to be run on the dataframe with the apply() func

    for col in columns:
        if callable(columns[col]):
            self.data[dataset][col] = \
                self.data[dataset].apply(columns[col], axis=1)
        else:
            self.data[dataset][col] = columns[col]

    report = 'Assigned ' + str(len(columns)) + ' to dataset'

    self.stepEnd(
        report=report,
        datasetName=dataset,
        df=self.data[dataset])


def pivotColsToRows(self,
                    dataset,
                    colsNotToPivot,
                    colsToPivot,
                    varName,
                    valueName,
                    desc):

    """
    Args:
        colsNotToPivot (list): Columns that should remain as columns (id_vars in Pandas terminology)
        colsToPivot (list): Columns to be pivoted to rows (value_vars in Pandas terminology)
        varName (string): The name of the column the pivoted column headings should go into
        valueName (string): The name of column the pivoted column values should go into
    """

    self.stepStart(desc=desc)

    auditCols = self.CONF.DATA.AUDIT_COLS['colNames'].tolist()

    self.data[dataset] = pd.melt(
        self.data[dataset],
        id_vars=colsNotToPivot + auditCols,
        value_vars=colsToPivot,
        var_name=varName,
        value_name=valueName)

    report = ''

    self.stepEnd(
        report=report,
        datasetName=dataset,  # optional
        df=self.data[dataset],  # optional
        shapeOnly=False)  # optional
