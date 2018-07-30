from betl.logger import logger


class Datastore():

    def __init__(self,
                 datastoreID,
                 datastoreType,
                 isSrcSys,
                 isSchemaDesc=False):

        if not isSrcSys:
            logger.logInitialiseDatastore(
                datastoreID,
                datastoreType,
                isSchemaDesc)

        self.datatoreID = datastoreID
        self.datastoreType = datastoreType
        self.isSrcSys = isSrcSys
