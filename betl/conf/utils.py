import sys
from betl.logger import cliText


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

        'REFRESH_SCHEMA': False,

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
                'EXT': True,
                'TRN': True,
                'BSE': True,
                'SUM': True}
        elif arg == 'rebuildext':
            params['RUN_REBUILDS']['EXT'] = True
        elif arg == 'rebuildtrn':
            params['RUN_REBUILDS']['TRN'] = True
        elif arg == 'rebuildbse':
            params['RUN_REBUILDS']['BSE'] = True
        elif arg == 'rebuildsum':
            params['RUN_REBUILDS']['SUM'] = True
        elif arg == 'refreshschema':
            params['REFRESH_SCHEMA'] = True
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
            params['DATA_LIMIT_ROWS'] = cliText.DATA_LIMIT_ROWS
        elif arg == 'run':
            params['RUN_DATAFLOWS'] = True
        elif arg == 'faillast':
            params['FAIL_LAST_EXEC'] = True
        else:
            isUnrecognisedArg = True
            unrecognisedArg = arg

    if isUnrecognisedArg and not showHelp:
        print(cliText.ARG_NOT_RECOGNISED.format(arg=unrecognisedArg))
        sys.exit()
    elif showHelp:
        print(cliText.HELP)
        sys.exit()
    else:
        # Check that bulk/delta set correctly
        if params['RUN_DATAFLOWS'] and \
           ((bulk and delta) or ((not bulk) and (not delta))):
            raise ValueError(cliText.BULK_OR_DELTA_NOT_SET)
        elif params['RUN_DATAFLOWS'] and bulk:
            if not skipWarnings:
                text = input(cliText.BULK_LOAD_WARNING)
                if text.lower() != 'y':
                    sys.exit()
                else:
                    print('')

        if params['RUN_RESET']:
            if not skipWarnings:
                text = input(cliText.SETUP_WARNING)
                if text.lower() != 'y':
                    sys.exit()
                else:
                    print('')

        if params['READ_SRC']:
            if not skipWarnings:
                text = input(cliText.READ_SRC_WARNING)
                if text.lower() != 'y':
                    sys.exit()
                else:
                    print('')

    return params
