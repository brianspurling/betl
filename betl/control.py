# betl imports
from . import schemas
from . import conf
from . import scheduler
from . import df_extract
from . import df_transform
from . import df_load
from . import utilities as utils
from . import setup
from . import cli

# 3rd Party imports
import sys

log = utils.setUpLogger('CONTRL', __name__)

# Global variables
RUN_SETUP = False
RUN_REBUILD_ALL = False
RUN_REBUILD_SRC = False
RUN_REBUILD_STG = False
RUN_REBUILD_TRG = False
RUN_REBUILD_SUM = False
RUN_EXTRACT = True
RUN_TRANSFORM = True
RUN_LOAD = True
DELETE_TMP_DATA = True
EXE_JOB = False
RERUN_PREV_JOB = False


#
# The main function that executes betl
#
def run():

    log.debug("START")

    global RERUN_PREV_JOB

    initialiseDBConnections()

    lastRunStatus = scheduler.getStatusOfLastExecution()
    if lastRunStatus['status'] == 'RUNNING' and EXE_JOB:
        text = input(cli.LAST_EXE_STILL_RUNNING)
        sys.exit()
    elif lastRunStatus['status'] != 'SUCCESSFUL' and EXE_JOB:
        text = input(cli.LAST_EXE_FAILED.format(
            status=lastRunStatus['status']))
        if text.lower() == 'ignore':
            RERUN_PREV_JOB = False
        else:
            RERUN_PREV_JOB = True

    if RERUN_PREV_JOB and (RUN_SETUP or
                           RUN_REBUILD_ALL or
                           RUN_REBUILD_SRC or
                           RUN_REBUILD_STG or
                           RUN_REBUILD_TRG or
                           RUN_REBUILD_SUM):
        text = input(cli.CANT_RERUN_AND_SETUP_OR_REBUILD)
        sys.exit()

    if RUN_SETUP:
        setupBetl()

    loadLogicalDataModels()

    if RUN_REBUILD_ALL:
        rebuildPhysicalDataModels()
    else:
        if RUN_REBUILD_SRC:
            rebuildPhysicalDataModels_src()
        if RUN_REBUILD_STG:
            rebuildPhysicalDataModels_stg()
        if RUN_REBUILD_TRG:
            rebuildPhysicalDataModel_trg()
        if RUN_REBUILD_SUM:
            rebuildPhysicalDataModel_sum()

    if EXE_JOB:

        scheduler.constructSchedule(RUN_EXTRACT, RUN_TRANSFORM, RUN_LOAD)

        if RERUN_PREV_JOB:
            jobId = lastRunStatus['jobId']
        else:
            if DELETE_TMP_DATA:
                utils.deleteTempoaryData()
            jobId = scheduler.addJobToJobLog()
            scheduler.writeScheduleToCntrlDb(jobId)

        scheduler.executeJob(jobId)

    log.debug("END")


#
# Initialise the connections to the various DBs & spreadsheets
#
def initialiseDBConnections():
    log.debug("START")

    utils.getCtlDBConnection()

    utils.getEtlDBConnection()
    utils.getEtlDBEngine()

    utils.getTrgDBConnection()
    utils.getTrgDBEngine()

    utils.getEtlSchemaConnection()
    utils.getTrgSchemaConnection()
    utils.getMsdConnection()

    log.debug("END")


#
# Set up the BETL control database
#
def setupBetl():
    log.debug("START")
    setup.setupBetl()
    log.debug("END")


#
# These need to be called in every execution.
# They create the Layer and DataModel objects
# that will be used throughout executation of
# the ETL, to interact with the persistent "layers"
# of the ETL database.
#
def loadLogicalDataModels():
    log.debug("START")
    loadLogicalDataModels_src()
    loadLogicalDataModels_stg()
    loadLogicalDataModels_trg()
    loadLogicalDataModels_sum()

    log.debug("END")


def loadLogicalDataModels_src():
    log.debug("START")
    schemas.SRC_LAYER = schemas.SrcLayer()
    log.debug("END")


def loadLogicalDataModels_stg():
    log.debug("START")
    schemas.STG_LAYER = schemas.StgLayer()
    log.debug("END")


def loadLogicalDataModels_trg():
    log.debug("START")
    schemas.TRG_LAYER = schemas.TrgLayer()
    log.debug("END")


def loadLogicalDataModels_sum():
    log.debug("START")
    schemas.SUM_LAYER = schemas.SumLayer()
    log.debug("END")


#
# These functions completely rebuild the
# corresonding pyshical data model
def rebuildPhysicalDataModels():
    log.debug("START")
    rebuildPhysicalDataModels_src()
    rebuildPhysicalDataModels_stg()
    rebuildPhysicalDataModel_trg()
    rebuildPhysicalDataModel_sum()
    log.debug("END")


#
# Create (or recreate) the ETL database's SRC layer
#
def rebuildPhysicalDataModels_src():
    log.debug("START")
    if conf.BULK_OR_DELTA == 'BULK':
        schemas.SRC_LAYER.rebuildPhsyicalDataModel()
    elif conf.BULK_OR_DELTA == 'DELTA':
        raise ValueError(cli.CANT_REBUILD_WITH_DELTA)
    log.debug("END")


#
# Create (or recreate) the ETL database's STG layer
#
def rebuildPhysicalDataModels_stg():
    log.debug("START")
    if conf.BULK_OR_DELTA == 'BULK':
        schemas.STG_LAYER.rebuildPhsyicalDataModel()
    elif conf.BULK_OR_DELTA == 'DELTA':
        raise ValueError(cli.CANT_REBUILD_WITH_DELTA)
    log.debug("END")


#
# Create (or recreate) the ETL database's TRG layer
#
def rebuildPhysicalDataModel_trg():
    log.debug("START")
    if conf.BULK_OR_DELTA == 'BULK':
        schemas.TRG_LAYER.rebuildPhsyicalDataModel()
    elif conf.BULK_OR_DELTA == 'DELTA':
        raise ValueError(cli.CANT_REBUILD_WITH_DELTA)
    log.debug("END")


#
# Create (or recreate) the ETL database's SUM layer
#
def rebuildPhysicalDataModel_sum():
    log.debug("START")
    if conf.BULK_OR_DELTA == 'BULK':
        schemas.SUM_LAYER.rebuildPhsyicalDataModel()
    elif conf.BULK_OR_DELTA == 'DELTA':
        raise ValueError(cli.CANT_REBUILD_WITH_DELTA)
    log.debug("END")


#
# Stick betl's default extraction data flow at the start of the schedule
#
# TODO: probably don't want to force this to position 0, more confusing this
# way than giving users full control based on the order in which they call
# these functions. Load, after all, can't be forced to the end.
def addDefaultExtractToSchedule(srcTablesToExclude=[]):
    log.debug("START")

    conf.SRC_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXTRACT = srcTablesToExclude

    scheduler.scheduleDataFlow(function=df_extract.defaultExtract,
                               etlStage='EXTRACT',
                               pos=0)
    log.debug("END")


#
# Stick betl's default load data flow at the end of the schedule
#
def addDefaultLoadToSchedule(nonDefaultStagingTables={}):
    log.debug("START")

    conf.TRG_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD = nonDefaultStagingTables

    scheduler.scheduleDataFlow(function=df_load.defaultLoad,
                               etlStage='LOAD')
    log.debug("END")


#
# Generate a standard DATE dimension
#
def addDMDateToSchedule():
    log.debug("START")
    scheduler.scheduleDataFlow(function=df_transform.generateDMDate,
                               etlStage='EXTRACT',
                               pos=0)
    log.debug("END")


#
# This function must be used by the calling application to
# process command line arguments
#
def processArgs(args):
    log.debug("START")

    global SKIP_WARNINGS
    global RUN_SETUP
    global RUN_REBUILD_ALL
    global RUN_REBUILD_SRC
    global RUN_REBUILD_STG
    global RUN_REBUILD_TRG
    global RUN_REBUILD_SUM
    global RUN_EXTRACT
    global RUN_TRANSFORM
    global RUN_LOAD
    global DELETE_TMP_DATA
    global EXE_JOB

    showHelp = False
    skipWarnings = False
    bulk = False
    delta = False
    isUnrecognisedArg = False

    for arg in args:
        if arg == 'help':
            showHelp = True
        elif arg == 'bulk':
            bulk = True
            conf.BULK_OR_DELTA = 'BULK'
        elif arg == 'delta':
            delta = True
            conf.BULK_OR_DELTA = 'DELTA'
        elif arg == 'nowarnings':
            skipWarnings = True
        elif arg == 'setup':
            RUN_SETUP = True
        elif arg == 'rebuildAll':
            RUN_REBUILD_ALL = True
        elif arg == 'rebuildSrc':
            RUN_REBUILD_SRC = True
        elif arg == 'rebuildStg':
            RUN_REBUILD_STG = True
        elif arg == 'rebuildTrg':
            RUN_REBUILD_TRG = True
        elif arg == 'rebuildSum':
            RUN_REBUILD_SUM = True
        elif arg == 'noextract':
            RUN_EXTRACT = False
        elif arg == 'notransform':
            RUN_TRANSFORM = False
        elif arg == 'noload':
            RUN_LOAD = False
        elif arg == 'retaintmpdata':
            DELETE_TMP_DATA = False
        elif arg == 'job':
            EXE_JOB = True
        else:
            if arg != sys.argv[0]:
                isUnrecognisedArg = True
                unrecognisedArg = arg

    if isUnrecognisedArg and not showHelp:
        print(cli.ARG_NOT_RECOGNISED.format(arg=unrecognisedArg))
        sys.exit()
    elif showHelp:
        print(cli.HELP)

        sys.exit()
    else:
        # Check that bulk/delta set correctly
        if (EXE_JOB and ((bulk and delta) or ((not bulk) and (not delta)))):
            raise ValueError(cli.BULK_OR_DELTA_NOT_SET)
        elif EXE_JOB and bulk:
            if not skipWarnings:
                text = input(cli.BULK_LOAD_WARNING)
                if text.lower() != 'y':
                    log.info('Betl execution quit by user')
                    sys.exit()
                else:
                    print('')

        if RUN_SETUP:
            if not skipWarnings:
                text = input(cli.SETUP_WARNING)
                if text.lower() != 'y':
                    log.info('Betl execution quit by user')
                    sys.exit()
                else:
                    print('')

    log.debug("END")
