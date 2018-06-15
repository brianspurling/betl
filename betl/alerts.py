FILE_NAME = None


def logAlert(conf, alerts):

    global FILE_NAME

    if FILE_NAME is None:
        execId = conf.STATE.EXEC_ID
        FILE_NAME = 'logs/' + str(execId).zfill(4) + '_alerts.txt'

    with open(FILE_NAME, 'a+') as f:
        f.write(alerts + '\n\n')
