from .SetupClass import Setup


def setupWithUserInput():

    print('\nBefore setting up BETL you will need a Google API Key file \n' +
          'in your current directory. Get your file from Google API ' +
          'Console. \nYou also need a Postgres server with the u/n & p/w ' +
          'for the default postgres database.' +
          '\n\nTo select default values, press enter.')

    tasks = getQuestionsForUserInput(Setup())

    for task in tasks:
        response = input('\n' + task['QU'] + ' ')
        task['FUNC'](response, task['DEFAULT'])

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


def getQuestionsForUserInput(setup):

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

    return tasks
