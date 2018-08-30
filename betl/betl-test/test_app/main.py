import betl
import sys

scheduleConfig = {

    # Control whether default transformations should be run
    'DEFAULT_EXTRACT': True,
    'DEFAULT_LOAD': True,
    'DEFAULT_SUMMARISE': True,

    # Data BETL can generate itself
    'DEFAULT_DM_DATE': True,
    'DEFAULT_DM_AUDIT': True,

    # Define tables to exclude from default processing
    'EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT': [],
    'BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD': [],

    # Here you define the bespoke parts of your data pipeline.
    'EXTRACT_DATAFLOWS': [
        dfl_example.exampleDataflow]
    'TRANSFORM_DATAFLOWS': [
    ],
    'LOAD_DATAFLOWS': [],
    'SUMMARISE_DATAFLOWS': []
}

pl = betl.pipeline(appConfigFile='./appConfig.ini',
                   scheduleConfig=scheduleConfig,
                   runTimeParams=sys.argv)

pl.run()
