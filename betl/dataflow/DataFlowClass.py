from betl.logger import logger
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

    from .dfl_mdm import (mapMasterData)

    from .dfl_merge import (join,
                            union)

    def __init__(self, conf, desc):

        self.dflStartTime = datetime.now()
        self.currentStepStartTime = None
        self.currentStepId = None
        self.conf = conf
        self.description = desc
        self.data = {}
        # trgDataset is always set to the most recent dataset written to disk
        self.trgDataset = None
        logger.logDFStart(self.description, self.dflStartTime)
        self.dataflowId = self.conf.CTRL.CTRL_DB.insertDataflow(
            dataflow={
                'execId': self.conf.STATE.EXEC_ID,
                'functionId': self.conf.STATE.FUNCTION_ID,
                'description': self.description})

    def stepStart(self,
                  desc,
                  datasetName=None,
                  df=None,
                  additionalDesc=None):

        self.currentStepStartTime = datetime.now()

        logger.logStepStart(
            startTime=self.currentStepStartTime,
            desc=desc,
            datasetName=datasetName,
            df=df,
            additionalDesc=additionalDesc)

        self.currentStepId = self.conf.CTRL.CTRL_DB.insertStep(
            step={
                'execId': self.conf.STATE.EXEC_ID,
                'dataflowID': self.dataflowId,
                'description': desc})

    def stepEnd(self,
                report,
                datasetName=None,
                df=None,
                shapeOnly=False):

        elapsedSeconds = \
            (datetime.now() - self.currentStepStartTime).total_seconds()

        logger.logStepEnd(
            report=report,
            duration=elapsedSeconds,
            datasetName=datasetName,
            df=df,
            shapeOnly=shapeOnly)

        if df is not None:
            rowCount = df.shape[0]
            colCount = df.shape[1]
        else:
            rowCount = None
            colCount = None

        self.conf.CTRL.CTRL_DB.updateStep(
            stepId=self.currentStepId,
            status='SUCCESSFUL',
            rowCount=rowCount,
            colCount=colCount)

    def close(self):
        elapsedSeconds = (datetime.now() - self.dflStartTime).total_seconds()
        logger.logDFEnd(elapsedSeconds, self.trgDataset)

        if self.trgDataset is not None:
            rowCount = self.trgDataset.shape[0]
            colCount = self.trgDataset.shape[1]
        else:
            rowCount = None
            colCount = None

        self.conf.CTRL.CTRL_DB.updateDataflow(
            dataflowId=self.dataflowId,
            status='SUCCESSFUL',
            rowCount=rowCount,
            colCount=colCount)

        # By removing all keys, we remove all pointers to the dataframes,
        # hence making them available to Python's garbage collection
        self.data.clear()
        del(self.trgDataset)

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
        op += 'DataFlow: ' + self.description + '\n'
        op += '  Datasets: \n'
        for dataset in self.data:
            op += '    - ' + dataset + '\n'
        op += '\n'
        return op
