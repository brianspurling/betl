def help():
    print(HELP)


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

TURN_OFF_TESTS_IF_NOT_WRITING_TO_DB = ("BETL must write to DB to be able to " +
                                       "run tests. Either turn off tests (" +
                                       "notests) or turn on DB write")

BULK_OR_DELTA_NOT_SET = ("Job must be either bulk or delta load")

BULK_LOAD_WARNING = ("\nRunning BULK load will completely wipe your " +
                     "data warehouse's history.\nAll changes stored " +
                     "by your deltas will be lost.\nSure? (Y or N)  ")

SETUP_WARNING = ("\nRunning RESET will completely reset BETL. " +
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
        "> [reset]\n" +
        "  Reset your pipeline's setup - all config will be lost\n" +
        "\n" +
        "> [readsrc]\n" +
        "  Auto-populate SRC layer schema descs from the source systems\n" +
        "\n" +
        "> [rebuildall | rebuildext | rebuildtrn | rebuildbse | " +
        "rebuildSum]\n" +
        "  Reconstruct the physical schema - all data will be lost\n" +
        "\n" +
        "> [refreshschema]\n" +
        "  Refresh the schema descriptions from Google Sheets\n" +
        "\n" +
        "> [bulk | delta]\n" +
        "  Specify whether we're running a bulk or delta\n" +
        "\n" +
        "> [run]\n" +
        "  Executes the job\n" +
        "\n" +
        "> [notests]\n" +
        "  Do not run any tests\n" +
        "\n" +
        "> [noextract] | [notransform] | [noload]\n" +
        "  Skip the extract / transform / load stage\n" +
        "\n" +
        "> [nodmload] | [noftload]\n" +
        "  Don't load the dimensions / fact tables\n" +
        "\n" +
        "> [nodbwrite]\n" +
        "  Do not write ETL data to physical DB (only write to CSV file)\n" +
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
        "To create a new pipeline, navigate to the root directory of your \n" +
        "new repo and run the following:  \n" +
        " $ python \n" +
        " $ import betl \n" +
        " $ betl.setupBetl() \n" +
        "\n" +
        "--------------------------------------------------------------\n" +
        "\n")
