FILE_NAME = None


def logAlert(conf, alert):

    global FILE_NAME

    if FILE_NAME is None:
        FILE_NAME = conf.LOG_PATH + '/' + '_alerts.txt'

    with open(FILE_NAME, 'a+') as f:
        f.write(alert + '\n\n')
