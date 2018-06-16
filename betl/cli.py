import sys


ARG_NOT_RECOGNISED = ("Argument {arg} not recognised. Try 'help'")

LAST_EXE_STILL_RUNNING = ("\nThe last execution of the job is still " +
                          "running.\nPress any key to abort the new execution")

LAST_EXE_FAILED = (
    "\nThe last execution of the job failed to complete.\n" +
    "It finished with status: {status}. \n" +
    "It is strongly recommended you complete the execution\n" +
    "before running a new load.\n\n" +
    "To ignore this warning and run a brand new load, enter\n" +
    "'ignore'. To rerun the previous job press any key\n\n")

CANT_RERUN_WITH_SETUP_OR_REBUILD = (
    "\nYou can't rerun a previous job at the same time as\n" +
    "reinstalling the betl config tables, or rebuilding a data\n" +
    "layer - you would lose all history of the last job run if\n" +
    "you did. Fool.\n\n" +
    "Press any key to abort execution.")

CANT_REBUILD_WITH_DELTA = ("You cannot rebuild the ETL database's data " +
                           "models as part of a delta load. Fool.")

BULK_OR_DELTA_NOT_SET = ("Job must be either bulk or delta load")

BULK_LOAD_WARNING = ("\nRunning BULK load will completely wipe your " +
                     "data warehouse's history.\nAll changes stored " +
                     "by your deltas will be lost.\nSure? (Y or N)  ")

SETUP_WARNING = ("\nRunning SETUP will completely reset BETL. " +
                 "\nAll execution logs in the Control DB will be lost, " +
                 "\nall tempoary data will be deleted, and log files will " +
                 "\nbe archived. \nSure? (Y or N)  ")

READ_SRC_WARNING = ("\nRunning READSRC will auto-populate your ETL DB " +
                    "\nschema description spreadsheet with a copy of the " +
                    "\nsource system(s) schemas. All current ETL.SRC. " +
                    "\nworksheets will be deleted or overwritten. " +
                    "\nSure? (Y or N)  ")

INVALID_STAGE_FOR_SCHEDULE = ("You can only schedule functions in one of " +
                              "the three ETL stages: EXTRACT, TRANSFORM, LOAD")

DATA_LIMIT_ROWS = 100

HELP = ("\n" +
        "--------------------------------------------------------------\n" +
        "\n" +
        "******************\n" +
        "* betl arguments *\n" +
        "******************\n" +
        "\n" +
        "> [setup]\n" +
        "  Reinstall betl - all config will be lost\n" +
        "\n" +
        "> [readsrc]\n" +
        "  Auto-populate SRC layer schema descs from the source systems\n" +
        "\n" +
        "> [rebuildall | rebuildsrc | rebuildstg | rebuildtrg | " +
        "rebuildSum]\n" +
        "  Reconstruct the physical data models - all data will be lost\n" +
        "\n" +
        "> [bulk | delta]\n" +
        "  Specify whether we're running a bulk or delta\n" +
        "\n" +
        "> [run]\n" +
        "  Executes the job\n" +
        "\n" +
        "> [noextract] | [notransform] | [noload]\n" +
        "  Skip the extract / transform / load stage\n" +
        "\n" +
        "> [nodmload] | [noftload]\n" +
        "  Don't load the dimensions / fact tables\n" +
        "\n" +
        "> [dbwrite]\n" +
        "  Write ETL data to physical DB (as opposed to only CSV files)\n" +
        "\n" +
        "> [cleartmpdata]\n" +
        "  Clear all temp data from previous jobs before executing\n" +
        "\n" +
        "> [limitdata]\n" +
        "  Limit the data to " + str(DATA_LIMIT_ROWS) + "\n" +
        "\n" +
        "> [nowarnings]\n" +
        "  Turn off warnings - only recommended during development\n" +
        "\n" +
        "> [loginfo | logdebug | logerror]\n" +
        "  The level of console logging output\n" +
        "\n" +
        "> [faillast]\n" +
        "  Marks the last exec as FAILED regarldess of its status\n" +
        "\n" +
        "*********************\n" +
        "* betl instructions *\n" +
        "*********************\n" +
        "\n" +
        "- In your main script, first setup your scheduleConfig \n" +
        "  dictionary (see documentation). This contains a list of all \n" +
        "  the bespoke dataflows your ETL script needs to run. \n" +
        "- Then call betl.init, passing in your appConfig file, your \n" +
        "  runtime parameters (sys.argv), and your scheduleConfig. \n" +
        " - Finally, call betl.run() \n" +
        "\n" +
        "--------------------------------------------------------------\n" +
        "\n")


def processArgs(args):

    showHelp = False
    skipWarnings = False
    bulk = False
    delta = False
    isUnrecognisedArg = False

    if len(args) > 0 and args[0] == 'main.py':
        del args[0]

    params = {

        'LOG_LEVEL': None,

        'RERUN_PREV_JOB': False,

        'SKIP_WARNINGS': False,

        'BULK_OR_DELTA': 'NOT SET',

        'RUN_SETUP': False,
        'READ_SRC': False,

        'RUN_REBUILDS': {},

        'RUN_EXTRACT': True,
        'RUN_TRANSFORM': True,
        'RUN_LOAD': True,
        'RUN_DM_LOAD': True,
        'RUN_FT_LOAD': True,
        'RUN_SUMMARISE': True,

        'WRITE_TO_ETL_DB': False,
        'DELETE_TMP_DATA': False,
        'DATA_LIMIT_ROWS': None,

        'RUN_DATAFLOWS': False,

        'FAIL_LAST_EXEC': False,

    }

    for arg in args:
        if arg == 'help':
            showHelp = True
        elif arg == 'log_info':
            params['LOG_LEVEL'] = 'INFO'
        elif arg == 'log_debug':
            params['LOG_LEVEL'] = 'DEBUG'
        elif arg == 'log_error':
            params['LOG_LEVEL'] = 'ERROR'
        elif arg == 'bulk':
            bulk = True
            params['BULK_OR_DELTA'] = 'BULK'
        elif arg == 'delta':
            delta = True
            params['BULK_OR_DELTA'] = 'DELTA'
        elif arg == 'nowarnings':
            skipWarnings = True
            params['SKIP_WARNINGS'] = True
        elif arg == 'setup':
            params['RUN_SETUP'] = True
        elif arg == 'readsrc':
            params['READ_SRC'] = True
        elif arg == 'rebuildall':
            params['RUN_REBUILDS'] = {
                'SRC': True,
                'STG': True,
                'TRG': True,
                'SUM': True}
        elif arg == 'rebuildsrc':
            params['RUN_REBUILDS']['SRC'] = True
        elif arg == 'rebuildstg':
            params['RUN_REBUILDS']['STG'] = True
        elif arg == 'rebuildtrg':
            params['RUN_REBUILDS']['TRG'] = True
        elif arg == 'rebuildsum':
            params['RUN_REBUILDS']['SUM'] = True
        elif arg == 'noextract':
            params['RUN_EXTRACT'] = False
        elif arg == 'notransform':
            params['RUN_TRANSFORM'] = False
        elif arg == 'noload':
            params['RUN_LOAD'] = False
        elif arg == 'nodmload':
            params['RUN_DM_LOAD'] = False
        elif arg == 'noftload':
            params['RUN_FT_LOAD'] = False
        elif arg == 'nosummarise':
            params['RUN_SUMMARISE'] = False
        elif arg == 'dbwrite':
            params['WRITE_TO_ETL_DB'] = True
        elif arg == 'cleartmpdata':
            params['DELETE_TMP_DATA'] = True
        elif arg == 'limitdata':
            params['DATA_LIMIT_ROWS'] = DATA_LIMIT_ROWS
        elif arg == 'run':
            params['RUN_DATAFLOWS'] = True
        elif arg == 'faillast':
            params['FAIL_LAST_EXEC'] = True
        else:
            isUnrecognisedArg = True
            unrecognisedArg = arg

    if isUnrecognisedArg and not showHelp:
        print(ARG_NOT_RECOGNISED.format(arg=unrecognisedArg))
        sys.exit()
    elif showHelp:
        print(HELP)
        sys.exit()
    else:
        # Check that bulk/delta set correctly
        if params['RUN_DATAFLOWS'] and \
           ((bulk and delta) or ((not bulk) and (not delta))):
            raise ValueError(BULK_OR_DELTA_NOT_SET)
        elif params['RUN_DATAFLOWS'] and bulk:
            if not skipWarnings:
                text = input(BULK_LOAD_WARNING)
                if text.lower() != 'y':
                    sys.exit()
                else:
                    print('')

        if params['RUN_SETUP']:
            if not skipWarnings:
                text = input(SETUP_WARNING)
                if text.lower() != 'y':
                    sys.exit()
                else:
                    print('')

        if params['READ_SRC']:
            if not skipWarnings:
                text = input(READ_SRC_WARNING)
                if text.lower() != 'y':
                    sys.exit()
                else:
                    print('')

    return params
