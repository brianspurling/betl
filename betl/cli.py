import sys
from .setup import Setup


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
        "To create a new pipeline, navigate to the root directory of your \n" +
        "new repo and run the following:  \n" +
        " $ python \n" +
        " $ import betl \n" +
        " $ betl.setup() \n" +
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

        'RUN_RESET': False,
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
        elif arg == 'reset':
            params['RUN_RESET'] = True
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

        if params['RUN_RESET']:
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


def runSetup():

    setup = Setup()

    # A series of questions and resulting functions to call.
    # Order matters.

    tasks = [

        # Values needed for appConfig.ini

        {'QU': '* Shortname for the data warehouse >>',
         'FUNC': setup.setDwhId},

        {'QU': '* Google API Key filename >>',
         'FUNC': setup.setGoogleAPIKeyFilename},

        {'QU': '* The Google Account you will use to manage the DWH >>',
         'FUNC': setup.setGoogleAccount},

        {'QU': '* Admin Postgres DB username >>',
         'FUNC': setup.setAdminPostgresUsername},

        {'QU': '* Admin Postgres DB password >>',
         'FUNC': setup.setAdminPostgresPassword},

        {'QU': 'Temp data path >>',
         'FUNC': setup.setTmpDataPath},

        {'QU': 'Source data path >>',
         'FUNC': setup.setSrcDataPath},

        {'QU': 'Reports path >>',
         'FUNC': setup.setReportsPath},

        {'QU': 'Logs path >>',
         'FUNC': setup.setLogsPath},

        {'QU': 'Control DB - host name >>',
         'FUNC': setup.setCtlDBHostName},

        {'QU': 'Control DB - database name >>',
         'FUNC': setup.setCtlDBName},

        {'QU': 'Control DB - username >>',
         'FUNC': setup.setCtlDBUsername},

        {'QU': 'Control DB - password >>',
         'FUNC': setup.setCtlDBPassword},

        {'QU': 'Schema description - ETL Google Sheets title >>',
         'FUNC': setup.setSchemaDescETLGsheetTitle},

        {'QU': 'Schema descriptions - TRG Google Sheets title >> >>',
         'FUNC': setup.setSchemaDescTRGGsheetTitle},

        {'QU': 'ETL DB - host name >>',
         'FUNC': setup.setETLDBHostName},

        {'QU': 'ETL DB - database name >>',
         'FUNC': setup.setETLDBName},

        {'QU': 'ETL DB - username >>',
         'FUNC': setup.setETLDBUsername},

        {'QU': 'ETL DB - password >>',
         'FUNC': setup.setETLDBPassword},

        {'QU': 'TRG DB - host name >>',
         'FUNC': setup.setTRGDBHostName},

        {'QU': 'TRG DB - database name >>',
         'FUNC': setup.setTRGDBName},

        {'QU': 'TRG DB - username >>',
         'FUNC': setup.setTRGDBUsername},

        {'QU': 'TRG DB - password >>',
         'FUNC': setup.setTRGDBPassword},

        {'QU': 'Default Rows - Google Sheets title >>',
         'FUNC': setup.setDefaultRowsGSheetTitle},

        {'QU': 'Master Data Mappings - Google Sheets title >>',
         'FUNC': setup.setMDMGSheetTitle},

        # Create the directories and default files

        {'QU': 'Create your directories? (Y/N) >>',
         'FUNC': setup.createDirectories},

        {'QU': 'Create your .gitignore file? (Y/N) >>',
         'FUNC': setup.createGitignoreFile},

        {'QU': 'Create your appConfig.ini file? (Y/N) >>',
         'FUNC': setup.createAppConfigFile},

        {'QU': 'Create your main.py script? (Y/N) >>',
         'FUNC': setup.createMainScript},

        {'QU': 'Create an example dataflow script? (Y/N) >>',
         'FUNC': setup.createExampleDataflow},

        # Create the database and GSheets

        {'QU': 'Create three Postgres databases: Ctrl, ETL and Target? ' +
               '(Y/N) ',
         'FUNC': setup.createDatabases},

        {'QU': 'Create two empty schema description Google Sheets: ' +
               'ETL and TRG? (Y/N)',
         'FUNC': setup.createSchemaDescGSheets},

        {'QU': 'Create an empty MDM (master data mapping) Google Sheet: (Y/N)',
         'FUNC': setup.createMDMGsheet},

        {'QU': 'Create an empty default rows Google Sheet: (Y/N)',
         'FUNC': setup.createDefaultRowsGsheet}

    ]

    print('\nBefore setting up BETL you will need a Google API Key file \n' +
          'in your current directory. Get your file from Google API ' +
          'Console. \nYou also need a Postgres server with the u/n & p/w ' +
          'for the default postgres database.' +
          '\n\nTo select default values, press enter.')

    for task in tasks:
        response = input('\n' + task['QU'] + ' ')
        task['FUNC'](response)

    print("\nWe've done as much as we can. Now you need to: \n" +
          "  - replace the example source systems in appConfig.ini with " +
          "your actual source system(s) config \n" +
          "  - run  `python main.py readsrc rebuildall bulk run` , " +
          "which will: \n" +
          "      - auto-populate the schema descriptions for your SRC " +
          "datalayer \n" +
          "      - create the physical data model in your ETL and TRG " +
          "databases \n" +
          "      - run the default bulk extract.\n" +
          "Then you can explore the source data in your ETL database, " +
          "define your \n" +
          "TRG data layer schema in Google Sheets, and start writing " +
          "your data pipeline\n\n")
