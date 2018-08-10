import betl.setup.setupUtils


def setupBetl(params=None):
    if params is None:
        setup = betl.setup.setupUtils.setupWithUserInput()
    else:
        setup = betl.setup.setupUtils.setup(params)

    return setup
