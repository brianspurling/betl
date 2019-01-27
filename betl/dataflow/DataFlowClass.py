from datetime import datetime


class DataFlow():

    # To keep the code maintainable, we have divied up the class' functions
    # across multiple modules. So we must import these all into the class.

    from .dfl_audit import (setAuditCols,
                            createAuditNKs)

    from .dfl_changeData import (setNulls,
                                 toNumeric,
                                 replace,
                                 setColumns)

    from .dfl_changeRow import (truncate,
                                dedupe,
                                filter,
                                filterWhereNotIn)

    from .dfl_changeSchema import (renameColumns,
                                   dropColumns,
                                   addColumns,
                                   pivotColsToRows)

    from .dfl_customCode import (customSQL,
                                 applyFunctionToColumns,
                                 applyFunctionToRows)

    from .dfl_io import (read,
                         write,
                         getDataFromSrc,
                         createDataset,
                         duplicateDataset,
                         getDataFrames,
                         getColumns,
                         getColumnList)

    from .dfl_loadPrep import (prepForLoad,
                               collapseNaturalKeyCols)

    from .dfl_mdm import (mapMasterData)

    from .dfl_merge import (join,
                            union)

    def __init__(self, desc, conf):

        self.dflStartTime = datetime.now()

        self.DESCRIPTION = desc
        self.CONF = conf

        self.currentStepStartTime = None
        self.currentStepId = None

        # The targetDataset is always the most recent dataset written to disk
        self.data = {}
        self.targetDataset = None

        self.CONF.log(
            'logDFStart',
            desc=desc,
            startTime=self.dflStartTime,
            stage=conf.STAGE)

    def stepStart(self,
                  desc,
                  datasetName=None,
                  df=None,
                  additionalDesc=None,
                  silent=False):

        self.currentStepStartTime = datetime.now()

        if not silent:
            self.CONF.log(
                'logStepStart',
                startTime=self.currentStepStartTime,
                desc=desc,
                datasetName=datasetName,
                df=df,
                additionalDesc=additionalDesc)

    def stepEnd(self,
                report,
                datasetName=None,
                df=None,
                shapeOnly=False,
                silent=False):

        elapsedSeconds = \
            (datetime.now() - self.currentStepStartTime).total_seconds()

        if not silent:
            self.CONF.log(
                'logStepEnd',
                report=report,
                duration=elapsedSeconds,
                datasetName=datasetName,
                df=df,
                shapeOnly=shapeOnly)

    def close(self):
        elapsedSeconds = (datetime.now() - self.dflStartTime).total_seconds()
        self.CONF.log(
            'logDFEnd',
            durationSeconds=elapsedSeconds,
            df=self.targetDataset)

        # By removing all keys, we remove all pointers to the dataframes,
        # hence making them available to Python's garbage collection
        self.data.clear()
        del(self.targetDataset)

    def templateStep(self, dataset, desc):

        self.stepStart(desc=desc)

        # self.data[dataset]

        report = ''

        self.stepEnd(
            report=report,
            datasetName=dataset,  # optional
            df=self.data[dataset],  # optional
            shapeOnly=False)  # optional

    def __str__(self):
        op = ''
        op += 'DataFlow: ' + self.DESCRIPTION + '\n'
        op += '  Datasets: \n'
        for dataset in self.data:
            op += '    - ' + dataset + '\n'
        op += '\n'
        return op
