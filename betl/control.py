# betl imports
from . import schemas
from . import conf
from . import scheduler
from . import df_extract
from . import utilities as utils
from . import setup

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
RUN_JOB = False


#
# Initialise the connections to the various DBs / STM
#
def initialiseDBConnections():
    log.debug("START")
    utils.getCtlDBConnection()
    utils.getEtlDBConnection()
    utils.getEtlDBEngine()
    utils.getStmConnection()
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
        raise ValueError("You cannot rebuild the ETL database's data models " +
                         "as part of a delta load. Fool.")
    log.debug("END")


#
# Create (or recreate) the ETL database's STG layer
#
def rebuildPhysicalDataModels_stg():
    log.debug("START")
    log.debug("END")


#
# Create (or recreate) the ETL database's TRG layer
#
def rebuildPhysicalDataModel_trg():
    log.debug("START")
    log.debug("END")


#
# Create (or recreate) the ETL database's SUM layer
#
def rebuildPhysicalDataModel_sum():
    log.debug("START")
    log.debug("END")


#
# Stick betl's default extraction data flow at the start of the schedule
#
def addDefaultExtractToSchedule():
    log.debug("START")
    scheduler.scheduleDataFlow(function=df_extract.df_defaultExtract,
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
    global RUN_JOB

    showHelp = False
    skipWarnings = False
    bulk = False
    delta = False
    unrecognisedArg = False

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
        elif arg == 'job':
            RUN_JOB = True
        else:
            if arg != sys.argv[0]:
                unrecognisedArg = True

    if unrecognisedArg and not showHelp:
        print("Argument " + arg + " not recognised. Try 'help'")
        sys.exit()
    elif showHelp:
        print("")
        print("--------------------------------------------------------------")
        print("")
        print("******************")
        print("* betl arguments *")
        print("******************")
        print("")
        print("> [setup]")
        print("  Reinstall betl - all config will be lost")
        print("")
        print("> [rebuildAll | rebuildSrc | rebuildStg | rebuildTrg |", end="")
        print(" rebuildSum]")
        print("  Reconstruct the physical data models - all data will be lost")
        print("")
        print("> bulk | delta")
        print("  Specify whether we're running a bulk or delta (required)")
        print("")
        print("> [job]")
        print("  Executes the job")
        print("")
        print("*********************")
        print("* betl instructions *")
        print("*********************")
        print("")
        print("- In your script, first call betl.processArgs(sys.argv)")
        print("- Then pass config details to betl with betl.loadAppConfig({})")
        print("  Refer to betl.conf.py for the configuration required")
        print("- Add your bespoke data flows to the schedule with")
        print("  betl.scheduleDataFlow(function,stage,pos)")
        print("- If the STM's SRC schema is empty, betl will auto-populate")
        print("  it from the source system(s)")
        print("- You will then need to identify the natural keys manually")
        print("- Use betl.useDefaultExtract() to use a standard extract:")
        print("  (It will use full table comparisons on the NKs to get deltas")
        print("")
        print("--------------------------------------------------------------")
        print("")
        sys.exit()
    else:
        # Check that bulk/delta set correctly
        if ((bulk and delta) or ((not bulk) and (not delta))):
            raise ValueError('Job must be either bulk or delta load')
        elif bulk:
            if not skipWarnings:
                text = input("\SURE you want to do a bulk load? Y or N:  ")
                if text != 'Y' and text != 'y':
                    log.info('Betl execution quit by user')
                    sys.exit()
                else:
                    print('')
    log.debug("END")


#
# The main function that executes betl
#
def run(args):

    log.debug("START")

    initialiseDBConnections()

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

    if RUN_JOB:
        scheduler.executeJob()

    log.debug("END")
