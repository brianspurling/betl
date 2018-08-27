FILE_NAME = None


def logAlert(conf, alert):

    global FILE_NAME

    if FILE_NAME is None:
        execId = conf.STATE.EXEC_ID
        FILE_NAME = conf.CTRL.LOG_PATH + '/' + str(execId).zfill(4) + '_alerts.txt'

    with open(FILE_NAME, 'a+') as f:
        f.write(alert + '\n\n')
