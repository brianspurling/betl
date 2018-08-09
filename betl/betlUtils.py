from .setupModule import setup
from .setupModule import setupWithUserInput


def setupBetl(params=None):
    if params is None:
        setupWithUserInput()
    else:
        setup(params)
