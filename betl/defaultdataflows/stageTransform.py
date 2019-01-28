def logTransformStart(**kwargs):
    kwargs['conf'].log('logTransformStart')


def logTransformEnd(**kwargs):
    kwargs['conf'].log('logTransformEnd')


def logSkipTransform(**kwargs):
    kwargs['conf'].log('logSkipTransform')
