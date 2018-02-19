ARG_NOT_RECOGNISED = ("Argument {arg} not recognised. Try 'help'")

LAST_EXE_STILL_RUNNING = ("\n\nThe last execution of the job is still " +
                          "running.\nPress any key to abort the new execution")

LAST_EXE_FAILED = ("\n\nThe last execution of the job failed to complete. " +
                   "It finished with status: {status}. It is strongly " +
                   "recommended you complete the execution " +
                   "before running a new load.\n\nTo ignore this warning " +
                   "and run a brand new load, enter 'ignore'\n")

CANT_RERUN_WITH_SETUP_OR_REBUILD = ("\n\nYou can't rerun a previous job at " +
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
                     "your deltas will be lost.\nSure? (Y or N)  ")

SETUP_WARNING = ("\nRunning SETUP will completely wipe your " +
                 "job's config.\nAll job logs will be lost.\n" +
                 "Sure? (Y or N)  ")

INVALID_STAGE_FOR_SCHEDULE = ("You can only schedule functions in one of " +
                              "the three ETL stages: EXTRACT, TRANSFORM, LOAD")

EXECUTION_SUCCESSFUL = ("\nBETL execution completed successfully. " +
                        "See {logFile} for logs\n\n")
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
        "> [rebuildAll | rebuildSrc | rebuildStg | rebuildTrg | " +
        "rebuildSum]\n" +
        "  Reconstruct the physical data models - all data will be lost\n" +
        "\n" +
        "> bulk | delta\n" +
        "  Specify whether we're running a bulk or delta (required)\n" +
        "\n" +
        "> [job]\n" +
        "  Executes the job\n" +
        "\n" +
        "> [noextract] | [notransform] | [noload]\n" +
        "  Skip the extract stage\n" +
        "\n" +
        "> [cleartmpdata]\n" +
        "  Clear all temp data from previous jobs before executing\n" +
        "\n" +
        "> [nowarnings]\n" +
        "  Turn off warnings - only recommended during development\n" +
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
