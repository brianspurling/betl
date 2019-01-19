FILE_NAME = None


def logAlert(betl, alert):

    global FILE_NAME

    if FILE_NAME is None:
        execId = betl.CONF.EXEC_ID
        FILE_NAME = betl.CONF.LOG_PATH + '/' + str(execId).zfill(4) + \
            '_alerts.txt'

    with open(FILE_NAME, 'a+') as f:
        f.write(alert + '\n\n')
