from betl.logger import Logger


class Datastore():

    def __init__(self,
                 datastoreID,
                 datastoreType,
                 isSrcSys,
                 isSchemaDesc=False):

        self.log = Logger()

        if not isSrcSys:
            self.log.logInitialiseDatastore(
                datastoreID,
                datastoreType,
                isSchemaDesc)

        self.datatoreID = datastoreID
        self.datastoreType = datastoreType
        self.isSrcSys = isSrcSys
