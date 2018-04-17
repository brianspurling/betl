import sys


ARG_NOT_RECOGNISED = ("Argument {arg} not recognised. Try 'help'")

LAST_EXE_STILL_RUNNING = ("\nThe last execution of the job is still " +
                          "running.\nPress any key to abort the new execution")

LAST_EXE_FAILED = ("\nThe last execution of the job failed to complete. " +
                   "It finished with status: {status}. It is strongly " +
                   "recommended you complete the execution " +
                   "before running a new load.\n\nTo ignore this warning " +
                   "and run a brand new load, enter 'ignore'. To rerun the " +
                   "previous job press any key\n")

CANT_RERUN_WITH_SETUP_OR_REBUILD = ("\nYou can't rerun a previous job at " +
                                    "the same time as reinstalling the betl " +
                                    "config tables, or rebuilding a data " +
                                    "layer - you would lose all history of " +
                                    "the last job run if you did. Fool.\n" +
                                    "Press any key to abort execution.")

CANT_REBUILD_WITH_DELTA = ("You cannot rebuild the ETL database's data " +
                           "models as part of a delta load. Fool.")

BULK_OR_DELTA_NOT_SET = ("Job must be either bulk or delta load")

BULK_LOAD_WARNING = ("\nRunning BULK load will completely wipe your " +
                     "data warehouse's history.\nAll changes stored " +
                     "by your deltas will be lost.\nSure? (Y or N)  ")

SETUP_WARNING = ("\nRunning SETUP will reset your control DB. " +
                 "\nAll execution logs in the DB will be lost (log files " +
                 "will be archived).\nSure? (Y or N)  ")

INVALID_STAGE_FOR_SCHEDULE = ("You can only schedule functions in one of " +
                              "the three ETL stages: EXTRACT, TRANSFORM, LOAD")

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
        "> [nowarnings]\n" +
        "  Turn off warnings - only recommended during development\n" +
        "\n" +
        "> [loginfo | logdebug | logerror]\n" +
        "  The level of console logging output\n" +
        "\n" +
        "*********************\n" +
        "* betl instructions *\n" +
        "*********************\n" +
        "\n" +
        "- In your script, first call betl.processArgs(sys.argv)\n" +
        "- Then pass config details to betl with betl.loadAppConfig()\n" +
        "  Refer to betl.conf.py for the configuration required\n" +
        "- Add your bespoke data flows to the schedule with\n" +
        "  betl.scheduleDataFlow(function,stage,pos)\n" +
        "- If the SRC schema def is empty, betl will auto-populate\n" +
        "  it from the source system(s)\n" +
        "- You will then need to identify the natural keys manually, in\n" +
        "  the spreadsheet\n" +
        "- Use betl.useDefaultExtract() to use a standard extract:\n" +
        "  (It will use full table comparisons on the NKs to get deltas\n" +
        "\n" +
        "--------------------------------------------------------------\n" +
        "\n")


def processArgs(args):

    showHelp = False
    skipWarnings = False
    bulk = False
    delta = False
    isUnrecognisedArg = False

    params = {

        'LOG_LEVEL': None,

        'RERUN_PREV_JOB': False,

        'SKIP_WARNINGS': False,

        'BULK_OR_DELTA': 'NOT SET',

        'RUN_SETUP': False,

        'RUN_REBUILD_ALL': False,
        'RUN_REBUILD_SRC': False,
        'RUN_REBUILD_STG': False,
        'RUN_REBUILD_TRG': False,
        'RUN_REBUILD_SUM': False,

        'RUN_EXTRACT': True,
        'RUN_TRANSFORM': True,
        'RUN_LOAD': True,
        'RUN_DM_LOAD': True,
        'RUN_FT_LOAD': True,

        'WRITE_TO_ETL_DB': False,
        'DELETE_TMP_DATA': False,

        'RUN_DATAFLOWS': False,

    }

    for arg in args[1:]:
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
        elif arg == 'rebuildall':
            params['RUN_REBUILD_ALL'] = True
        elif arg == 'rebuildsrc':
            params['RUN_REBUILD_SRC'] = True
        elif arg == 'rebuildstg':
            params['RUN_REBUILD_STG'] = True
        elif arg == 'rebuildtrg':
            params['RUN_REBUILD_TRG'] = True
        elif arg == 'rebuildsum':
            params['RUN_REBUILD_SUM'] = True
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
        elif arg == 'dbwrite':
            params['WRITE_TO_ETL_DB'] = True
        elif arg == 'cleartmpdata':
            params['DELETE_TMP_DATA'] = True
        elif arg == 'run':
            params['RUN_DATAFLOWS'] = True
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

    return params
