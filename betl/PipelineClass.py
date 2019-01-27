import os
from .defaultdataflows import stageSetup
from configobj import ConfigObj
from airflow.operators.python_operator import PythonOperator
from betl.defaultdataflows import stageExtract
from betl.defaultdataflows import stageTransform
from betl.defaultdataflows import stageLoad
from betl.defaultdataflows import stageSummarise
from betl.defaultdataflows import dmDate
from betl.defaultdataflows import dmAudit
from .conf import Conf
from betl.logger import Logger


class Pipeline():

    def __init__(self, appDirectory, appConfigFile, scheduleConfig, dag=None):

        self.DAG = dag
        if self.DAG:
            isAirflow = True
        else:
            isAirflow = False

        appDirectory = os.path.expanduser(appDirectory)

        #################################
        # PROCESS CONFIGURATION OPTIONS #
        #################################

        conf = {
            'appDirectory': appDirectory,
            'scheduleConfig': scheduleConfig,
            'isAdmin': False,
            'isAirflow': isAirflow}

        if appConfigFile is None:
            conf['appConfig'] = Conf.defaultAppConfig
        else:
            conf['appConfig'] = ConfigObj(appDirectory + appConfigFile)

        if scheduleConfig is None:
            conf['scheduleConfig'] = Conf.defaultScheduleConfig

        ####################
        # INIT CONF OBJECT #
        ####################

        # TODO: Even though I tested it once and it seemed to work, I'm seeing
        # issues with CONF.LOG (and it makes more sense generally) that the
        # CONF object passed from this class to every operator is not the same
        # object, therefore changing it in one op does not change it in aother
        # Therefore loggging needs moving.
        # Further to this, though... there are bits within conf.init which
        # _are_ working, becuase they are run by pipeline before passing to
        # each op indivually, but are probably undue overheads and should be
        # move to xcoms - e.g. the logical data model
        self.CONF = Conf(conf)

        ######################
        # CONSTRUCT PIPELINE #
        ######################

        # If the DAG param has been passed in, the pipeline - a series of
        # operators (createOp()) - will constructed as an Airflow DAG -- i.e.
        # it will not be executed until the DAG is triggered by Airflow. If no
        # DAG param has been passed, these operators will be executed
        # immediately

        logBETLStart = self.createOp(
            taskId='logBETLStart',
            func=stageSetup.logBETLStart,
            test='hello world')

        if self.CONF.RUN_DATAFLOWS:

            if self.CONF.RUN_EXTRACT:

                # Bespoke extract DFs are run in parallel
                # with the default extract DF

                logExtractStart = self.createOp(
                    taskId='logExtractStart',
                    func=stageExtract.logExtractStart,
                    upstream=logBETLStart)

                extractOps = []

                if self.CONF.DEFAULT_EXTRACT:

                    if self.CONF.BULK_OR_DELTA == 'BULK':

                        # for convenience
                        extLayer = self.CONF.getLogicalSchemaDataLayer('EXT')
                        skip = self.CONF.EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT
                        for dmId in extLayer.datasets:
                            for tableName in extLayer.datasets[dmId].tables:

                                if tableName in skip:
                                    continue

                                extractOp = self.createOp(
                                    taskId='bulkExtract_' + tableName,
                                    func=stageExtract.bulkExtract,
                                    upstream=logExtractStart,
                                    tableName=tableName,
                                    dmId=dmId)
                                extractOps.append(extractOp)

                    elif self.CONF.BULK_OR_DELTA == 'DELTA':
                        pass  # TODO! Some code already written

                leafOps = self.createAndScheduleDFOperators(
                    dfs=self.CONF.EXTRACT_DATAFLOWS,
                    upstream=logExtractStart)

                extractOps.append(leafOps)

                logExtractEnd = self.createOp(
                    taskId='logExtractEnd',
                    func=stageExtract.logExtractEnd,
                    upstream=extractOps)

            else:

                logExtractEnd = self.createOp(
                    taskId='logSkipExtract',
                    func=stageExtract.logSkipExtract,
                    upstream=logBETLStart)

            # Bespoke transform DFs are run in parallel with the default DFs
            if self.CONF.RUN_TRANSFORM:

                logTransformStart = self.createOp(
                    taskId='logTransformStart',
                    func=stageTransform.logTransformStart,
                    upstream=logExtractEnd)

                leafOps = []

                if self.CONF.DEFAULT_DM_DATE:
                    transformDMDate = self.createOp(
                        taskId='transformDMDate',
                        func=dmDate.transformDMDate,
                        upstream=logTransformStart)
                    leafOps.append(transformDMDate)

                if self.CONF.DEFAULT_DM_AUDIT:
                    transformDMAudit = self.createOp(
                        taskId='transformDMAudit',
                        func=dmAudit.transformDMAudit,
                        upstream=logTransformStart)
                    leafOps.append(transformDMAudit)

                leafOps = leafOps + self.createAndScheduleDFOperators(
                    dfs=self.CONF.TRANSFORM_DATAFLOWS,
                    upstream=logTransformStart)

                logTransformEnd = self.createOp(
                    taskId='logTransformEnd',
                    func=stageTransform.logTransformEnd,
                    upstream=leafOps)

            else:

                logTransformEnd = self.createOp(
                    taskId='logSkipTransform',
                    func=stageSetup.logSkipTransform,
                    upstream=logExtractEnd)

            # Bespoke transform DFs are run after the default load DFs
            if self.CONF.RUN_LOAD:

                # TODO: need error catching, if admin hasn't been run there
                # won't be any datamodels to parse
                bseLayer = self.CONF.getLogicalSchemaDataLayer('BSE')
                bseTables = bseLayer.datasets['BSE'].tables
                skip = self.CONF.BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD

                loadStartOp = self.createOp(
                    taskId='logLoadStart',
                    func=stageLoad.logLoadStart,
                    upstream=logTransformEnd)

                if self.CONF.BULK_OR_DELTA == 'BULK':

                    logBulkLoadSetupStart = self.createOp(
                        taskId='logBulkLoadSetupStart',
                        func=stageLoad.logBulkLoadSetupStart,
                        upstream=loadStartOp)

                    refreshDefaultRowsTxtFileFromGSheet = self.createOp(
                        taskId='refreshDefaultRowsTxtFileFromGSheet',
                        func=stageLoad.refreshDefaultRowsTxtFileFromGSheet,
                        upstream=logBulkLoadSetupStart)

                    dropFactFKConstraints = self.createOp(
                        taskId='dropFactFKConstraints',
                        func=stageLoad.dropFactFKConstraints,
                        upstream=refreshDefaultRowsTxtFileFromGSheet)

                    logBulkLoadSetupEnd = self.createOp(
                        taskId='logBulkLoadSetupEnd',
                        func=stageLoad.logBulkLoadSetupEnd,
                        upstream=dropFactFKConstraints)

                    loadStartOp = logBulkLoadSetupEnd
                # We must load the dimensions before the facts, so loop
                # through tables twice - once for dims, once for facts
                # This could be more concise (single loop with more conditions)
                # but we need to call the createOps in the correct order for
                # non Airflow execution

                # DIMENSIONS

                logDimLoadStart = self.createOp(
                    taskId='logDimLoadStart',
                    func=stageLoad.logDimLoadStart,
                    upstream=loadStartOp)

                logDefaultDimLoadStart = self.createOp(
                    taskId='logDefaultDimLoadStart',
                    func=stageLoad.logDefaultDimLoadStart,
                    upstream=logDimLoadStart)

                dimOps = []
                for tableName in bseTables:

                    if tableName in skip:
                        continue
                    if bseTables[tableName].getTableType() != 'DIMENSION':
                        continue

                    if self.CONF.BULK_OR_DELTA == 'BULK':

                        op = self.createOp(
                            taskId='bulkLoad_' + tableName,
                            func=stageLoad.bulkLoad,
                            upstream=logDefaultDimLoadStart,
                            args=[
                                tableName,
                                bseTables[tableName],
                                'DIMENSION'])
                        dimOps.append(op)

                    elif self.CONF.BULK_OR_DELTA == 'DELTA':

                        self.createOp(
                            taskId='deltaLoad_' + tableName,
                            func=stageLoad.deltaLoad,
                            upstream=logDefaultDimLoadStart,
                            args=[
                                tableName,
                                bseTables[tableName],
                                'DIMENSION'])
                        dimOps.append(op)

                prevOp = logDefaultDimLoadStart
                if len(dimOps) > 0:
                    prevOp = dimOps

                logDefaultDimLoadEnd = self.createOp(
                    taskId='logDefaultDimLoadEnd',
                    func=stageLoad.logDefaultDimLoadEnd,
                    upstream=prevOp)

                logBespokeDimLoadStart = self.createOp(
                    taskId='logBespokeDimLoadStart',
                    func=stageLoad.logBespokeDimLoadStart,
                    upstream=logDefaultDimLoadEnd)

                leafOps = self.createAndScheduleDFOperators(
                    dfs=self.CONF.LOAD_DIM_DATAFLOWS,  # Bespoke dim load DFs
                    upstream=logBespokeDimLoadStart)

                prevOp = logBespokeDimLoadStart
                if len(leafOps) > 0:
                    prevOp = leafOps

                logBespokeDimLoadEnd = self.createOp(
                    taskId='logBespokeDimLoadEnd',
                    func=stageLoad.logBespokeDimLoadEnd,
                    upstream=prevOp)

                logDimLoadEnd = self.createOp(
                    taskId='logDimLoadEnd',
                    func=stageLoad.logDimLoadEnd,
                    upstream=logBespokeDimLoadEnd)

                # FACTS

                logFactLoadStart = self.createOp(
                    taskId='logFactLoadStart',
                    func=stageLoad.logFactLoadStart,
                    upstream=logDimLoadEnd)

                logDefaultFactLoadStart = self.createOp(
                    taskId='logDefaultFactLoadStart',
                    func=stageLoad.logDefaultFactLoadStart,
                    upstream=logFactLoadStart)

                factOps = []
                for tableName in bseTables:

                    if tableName in skip:
                        continue
                    if bseTables[tableName].getTableType() != 'FACT':
                        continue

                    if self.CONF.BULK_OR_DELTA == 'BULK':

                        op = self.createOp(
                            taskId='bulkLoad_' + tableName,
                            func=stageLoad.bulkLoad,
                            upstream=logDefaultFactLoadStart,
                            args=[tableName, bseTables[tableName], 'FACT'])
                        factOps.append(op)

                    elif self.CONF.BULK_OR_DELTA == 'DELTA':

                        self.createOp(
                            taskId='deltaLoad_' + tableName,
                            func=stageLoad.deltaLoad,
                            upstream=logDefaultFactLoadStart,
                            args=[tableName, bseTables[tableName], 'FACT'])
                        factOps.append(op)

                prevOp = logDefaultFactLoadStart
                if len(factOps) > 0:
                    prevOp = factOps

                logDefaultFactLoadEnd = self.createOp(
                    taskId='logDefaultFactLoadEnd',
                    func=stageLoad.logDefaultFactLoadEnd,
                    upstream=prevOp)

                logBespokeFactLoadStart = self.createOp(
                    taskId='logBespokeFactLoadStart',
                    func=stageLoad.logBespokeFactLoadStart,
                    upstream=logDefaultFactLoadEnd)

                leafOps = self.createAndScheduleDFOperators(
                    dfs=self.CONF.LOAD_FACT_DATAFLOWS,  # Bespoke fact load DFs
                    upstream=logBespokeFactLoadStart)

                prevOp = logBespokeFactLoadStart
                if len(leafOps) > 0:
                    prevOp = leafOps

                logBespokeFactLoadEnd = self.createOp(
                    taskId='logBespokeFactLoadEnd',
                    func=stageLoad.logBespokeFactLoadEnd,
                    upstream=prevOp)

                logFactLoadEnd = self.createOp(
                    taskId='logFactLoadEnd',
                    func=stageLoad.logFactLoadEnd,
                    upstream=logBespokeFactLoadEnd)

                logLoadEnd = self.createOp(
                    taskId='logLoadEnd',
                    func=stageLoad.logLoadEnd,
                    upstream=logFactLoadEnd)

            else:

                logLoadEnd = self.createOp(
                    taskId='logSkipLoad',
                    func=stageSetup.logSkipLoad,
                    upstream=logTransformEnd)

            # Bespoke summarise DFs are run in between the default
            # prep/finish DFs
            if self.CONF.RUN_SUMMARISE:

                summariseStart = self.createOp(
                    taskId='logSummariseStart',
                    func=stageSummarise.logSummariseStart,
                    upstream=logLoadEnd)

                if self.CONF.DEFAULT_SUMMARISE:
                    defaultSummarisePrep = self.createOp(
                        taskId='defaultSummarisePrep',
                        func=stageSummarise.defaultSummarisePrep,
                        upstream=summariseStart)

                logBespokeSummariseStart = self.createOp(
                    taskId='logBespokeSummariseStart',
                    func=stageSummarise.logBespokeSummariseStart,
                    upstream=defaultSummarisePrep)

                leafOps = self.createAndScheduleDFOperators(
                    dfs=self.CONF.SUMMARISE_DATAFLOWS,
                    upstream=logBespokeSummariseStart)

                logBespokeSummariseEnd = self.createOp(
                    taskId='logBespokeSummariseEnd',
                    func=stageSummarise.logBespokeSummariseEnd,
                    upstream=leafOps)

                logSummariseEnd = self.createOp(
                    taskId='logSummariseEnd',
                    func=stageSummarise.logSummariseEnd,
                    upstream=logBespokeSummariseEnd)

            else:

                logSummariseEnd = self.createOp(
                    taskId='logSkipSummarise',
                    func=stageSummarise.logSkipSummarise,
                    upstream=logLoadEnd)

            # END

            logExecutionEnd_op = self.createOp(
                taskId='logExecutionEnd',
                func=logExecutionEnd,
                upstream=logSummariseEnd)

        self.createOp(
            taskId='logBETLEnd',
            func=logBETLEnd,
            upstream=logExecutionEnd_op)

    def createAndScheduleDFOperators(self,
                                     dfs,
                                     upstream):

        ops = []

        for funcID in dfs:

            op = self.createOp(
                taskId=funcID,
                func=dfs[funcID]['func'],
                isBETLFunc=False)

            # Store operator in dict so we can reference for depednencies
            # and a list so we can return all ops
            dfs[funcID]['op'] = op
            ops.append(op)

            # If this is an Airflow execution, set the upstream ops
            if self.DAG is not None:
                if ('upstream' in dfs[funcID] and
                        len(dfs[funcID]['upstream']) > 0):
                    for upstreamFuncID in dfs[funcID]['upstream']:
                        op.set_upstream(dfs[upstreamFuncID]['op'])
                else:
                    op.set_upstream(upstream)

        leafOps = []
        if self.DAG is not None:
            for op in ops:
                if len(op.downstream_list) == 0:
                    leafOps.append(op)
        return leafOps

    def createOp(self, taskId, func, isBETLFunc=True, upstream=[], **kwargs):

        if self.DAG is not None:
            if not isinstance(upstream, list):
                upstream = [upstream]

            # Add the func to the dict of args so we can pass the whole
            # thing through to the PythonOperator
            op_kwargs = {
                **kwargs,
                'func': func,
                'conf': self.CONF,
                'isBETLFunc': isBETLFunc}

            op = PythonOperator(
                task_id=taskId,
                python_callable=wrapperFunc,
                dag=self.DAG,
                provide_context=True,
                op_kwargs=op_kwargs)  # provide_context=True)

            for upstreamOps in upstream:
                op.set_upstream(upstreamOps)

            return op
        else:
            # We call the conf arg "betl" for the app-facing interface
            if isBETLFunc:
                func(**{**kwargs, 'conf': self.CONF})
            else:
                func(**{**kwargs, 'betl': self.CONF})

    def log(self, logMethod, **kwargs):
        getattr(self.LOG, logMethod)(**kwargs)


# When executed by airflow, this code runs before every task
def wrapperFunc(**kwargs):

    # Get the airflow run ID. The BETL logger uses this to name the log file
    if 'run_id' in kwargs:
        run_id = kwargs['run_id']
    if run_id is not None:
        kwargs['conf'].EXEC_ID = run_id
    else:
        kwargs['conf'].EXEC_ID = 'test'

    # set up logger
    kwargs['conf'].LOG = Logger(kwargs['conf'])

    # App functions (i.e. bespoke functions) cannot define their own params,
    # they all just get passed the conf objects
    # BETL funcs, on the other hand, e.g. default data flows, can have
    # additional params. Note that the entire Airflow context is passed through
    # in kwargs as well
    if kwargs['isBETLFunc']:
        kwargs['func'](**kwargs)
    else:
        kwargs['func'](kwargs['conf'])


def logExecutionStart(conf):
    conf.log('logExecutionStart')


def logExecutionEnd(conf):
    conf.log('logExecutionEnd')


def logBETLEnd(conf):
    conf.log('logBETLEnd')
