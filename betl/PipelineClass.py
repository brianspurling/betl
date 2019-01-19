from .dataflow import DataFlow
from .conf import Conf
from .reporting import reporting
from .defaultdataflows import stageSetup
from configobj import ConfigObj
from betl.datamodel import DataLayer
from airflow.operators.python_operator import PythonOperator
from betl.defaultdataflows import stageExtract
from betl.defaultdataflows import stageTransform
from betl.defaultdataflows import stageLoad
from betl.defaultdataflows import stageSummarise
from betl.defaultdataflows import dmDate
from betl.defaultdataflows import dmAudit
from airflow.operators.python_operator import PythonOperator

class Pipeline():

    def __init__(self, appConfigFile, scheduleConfig, dag):

        self.DAG = dag

        #################################
        # PROCESS CONFIGURATION OPTIONS #
        #################################

        if appConfigFile is None:
            appConfig = Conf.defaultAppConfig
        else:
            appConfig = ConfigObj(appConfigFile)

        if scheduleConfig is None:
            scheduleConfig = Conf.defaultScheduleConfig


        ####################
        # INIT CONF OBJECT #
        ####################

        self.CONF = Conf(
            betl=self,
            appConfig=appConfig,
            scheduleConfig=scheduleConfig)

        #######################
        # LOGICAL DWH SCHEMAS #
        #######################

        for dlId in self.CONF.dataLayers:
            self.CONF.DWH_LOGICAL_SCHEMAS[dlId] = \
                DataLayer(
                    betl=self,
                    dataLayerID=dlId)

        ##############################
        # ADD SETUP OPERATORS TO DAG #
        ##############################

        ## INIT BETL

        # Creates betl.LOG & betl.CONTROL_DB, runs full reset if requested
        initBETL = self.createOp(
            taskId='initBETL',
            func=stageSetup.initBETL)

        ## VALIDATE SCHEDULE
        validateSchedule = self.createOp(
            taskId='validateSchedule',
            func=stageSetup.validateSchedule,
            upstream=initBETL)

        ## SETUP BETL

        logSetupStart = self.createOp(
            taskId='logSetupStart',
            func=stageSetup.logSetupStart,
            upstream=validateSchedule)

        logSetupEnd = self.createOp(
            taskId='logSetupEnd',
            func=stageSetup.logSetupEnd)

        # TEMP DATA FILE MAPPING

        populateTempDataFileNameMap = self.createOp(
            taskId='populateTempDataFileNameMap',
            func=stageSetup.populateTempDataFileNameMap,
            upstream=logSetupStart,
            downstream=logSetupEnd)

        #################################
        # ADD DATAFLOW OPERATORS TO DAG #
        #################################

        if self.CONF.RUN_DATAFLOWS:

            logExecutionStart = self.createOp(
                taskId='logExecutionStart',
                func=self.logExecutionStart,
                upstream=logSetupEnd)

            # Bespoke extract DFs are run in parallel with the default extract DF
            if self.CONF.RUN_EXTRACT:

                logExtractStart = self.createOp(
                    taskId='logExtractStart',
                    func=stageExtract.logExtractStart,
                    upstream=logExecutionStart)

                logExtractEnd = self.createOp(
                    taskId='logExtractEnd',
                    func=stageExtract.logExtractEnd)

                if self.CONF.DEFAULT_EXTRACT:

                    if self.CONF.BULK_OR_DELTA == 'BULK':

                        # for convenience
                        extLayer = self.CONF.getLogicalSchemaDataLayer('EXT')

                        for dmID in extLayer.datasets:
                            for tableName in extLayer.datasets[dmID].tables:

                                mappedTableName = extLayer.datasets[dmID].tables[tableName].srcTableName
                                if tableName in self.CONF.EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT:
                                    continue

                                bulkExtract = self.createOp(
                                    taskId='bulkExtract_' + tableName,
                                    func=stageExtract.bulkExtract,
                                    upstream=logExtractStart,
                                    downstream=logExtractEnd,
                                    args=[tableName, dmID])

                    elif self.CONF.BULK_OR_DELTA == 'DELTA':
                        pass  # TODO! Some code already written

                self.createAndScheduleDFOperators(
                    dfs=self.CONF.EXTRACT_DATAFLOWS,
                    startOperator=logExtractStart,
                    endOperator=logExtractEnd)

            else:

                logExtractEnd = self.createOp(
                    taskId='logSkipExtract',
                    func=stageExtract.logSkipExtract,
                    upstream=logExecutionStart)

            # Bespoke transform DFs are run in parallel with the default DFs
            if self.CONF.RUN_TRANSFORM:

                logTransformStart = self.createOp(
                    taskId='logTransformStart',
                    func=stageTransform.logTransformStart,
                    upstream=logExtractEnd)

                logTransformEnd = self.createOp(
                    taskId='logTransformEnd',
                    func=stageTransform.logTransformEnd)

                if self.CONF.DEFAULT_DM_DATE:
                    transformDMDate = self.createOp(
                        taskId='transformDMDate',
                        func=dmDate.transformDMDate,
                        upstream=logTransformStart,
                        downstream=logTransformEnd)

                if self.CONF.DEFAULT_DM_AUDIT:
                    transformDMAudit = self.createOp(
                        taskId='transformDMAudit',
                        func=dmAudit.transformDMAudit,
                        upstream=logTransformStart,
                        downstream=logTransformEnd)

                self.createAndScheduleDFOperators(
                    dfs=self.CONF.TRANSFORM_DATAFLOWS,
                    startOperator=logTransformStart,
                    endOperator=logTransformEnd)

            else:

                logTransformEnd = self.createOp(
                    taskId='logSkipTransform',
                    func=stageSetup.logSkipTransform,
                    upstream=logExtractEnd)

            # Bespoke transform DFs are run after the default load DFs
            if self.CONF.RUN_LOAD:

                logLoadStart = self.createOp(
                    taskId='logLoadStart',
                    func=stageLoad.logLoadStart,
                    upstream=logTransformEnd)
                loadStart = logLoadStart

                logLoadEnd = self.createOp(
                    taskId='logLoadEnd',
                    func=stageLoad.logLoadEnd)

                if self.CONF.DEFAULT_LOAD:
                    defaultLoad = self.createOp(
                        taskId='defaultLoad',
                        func=stageLoad.defaultLoad,
                        upstream=logLoadStart)
                    loadStart = defaultLoad

                self.createAndScheduleDFOperators(
                    dfs=self.CONF.LOAD_DATAFLOWS,
                    startOperator=loadStart,
                    endOperator=logLoadEnd)

            else:

                logLoadEnd = self.createOp(
                    taskId='logSkipLoad',
                    func=stageSetup.logSkipLoad,
                    upstream=logTransformEnd)

            # Bespoke summarise DFs are run in between the default prep/finish DFs
            if self.CONF.RUN_SUMMARISE:

                logSummariseStart = self.createOp(
                    taskId='logSummariseStart',
                    func=stageSummarise.logSummariseStart,
                    upstream=logLoadEnd)
                summariseStart = logSummariseStart

                logSummariseEnd = self.createOp(
                    taskId='logSummariseEnd',
                    func=stageSummarise.logSummariseEnd)

                if self.CONF.DEFAULT_SUMMARISE:
                    defaultSummarisePrep = self.createOp(
                        taskId='defaultSummarisePrep',
                        func=stageSummarise.defaultSummarisePrep,
                        upstream=logSummariseStart)
                    summariseStart = defaultSummarisePrep

                self.createAndScheduleDFOperators(
                    dfs=self.CONF.SUMMARISE_DATAFLOWS,
                    startOperator=summariseStart,
                    endOperator=logSummariseEnd)

            else:

                logSummariseEnd = self.createOp(
                    taskId='logSkipSummarise',
                    func=stageSetup.logSkipSummarise,
                    upstream=logLoadEnd)

            ## END

            logExecutionEnd = self.createOp(
                taskId='logExecutionEnd',
                func=self.logExecutionEnd,
                upstream=logSummariseEnd)

        logBETLEnd = self.createOp(
            taskId='logBETLEnd',
            func=self.logBETLEnd,
            upstream=logExecutionEnd)


    def createAndScheduleDFOperators(self,
                                   dfs,
                                   startOperator,
                                   endOperator):

        for funcID in dfs:

            op = self.createOp(
                taskId=funcID,
                func=dfs[funcID]['func'])

            # Store operator in dict so we can reference for depednencies
            dfs[funcID]['op'] = op

            if 'upstream' in dfs[funcID] and len(dfs[funcID]['upstream']) > 0:
                for upstreamFuncID in dfs[funcID]['upstream']:
                    op.set_upstream(dfs[upstreamFuncID]['op'])
            else:
                op.set_upstream(startOperator)

        # Now we have set all _upstream_ dependencies, loop back through
        # all our operators and set any without a downstream dependency to
        # feed into the endOperator
        for funcID in dfs:
            if len(dfs[funcID]['op'].downstream_list) == 0:
                dfs[funcID]['op'].set_downstream(endOperator)

    def createOp(self, taskId, func, upstream=[], downstream=[], args=[]):

        if not isinstance(upstream, list):
            upstream = [upstream]
        if not isinstance(downstream, list):
            downstream = [downstream]
        if not isinstance(args, list):
            args = [args]

        if len(args) > 0:
            args = [self] + args

        op = PythonOperator(
            task_id=taskId,
            python_callable=func,
            dag=self.DAG,
            op_args=args)

        for func in upstream:
            op.set_upstream(func)

        for func in downstream:
            op.set_downstream(func)

        return op

    def logExecutionStart(betl):
        self.LOG.logExecutionStart()

    def logExecutionEnd(betl):
        self.LOG.logExecutionEnd()

    def logBETLEnd(betl):
        self.LOG.logBETLEnd()

    def DataFlow(self, desc):
        return DataFlow(desc=desc, conf=self.CONF)
