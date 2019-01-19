class Datastore():

    def __init__(self,
                 datastoreID,
                 datastoreType,
                 isSrcSys,
                 isSchemaDesc=False):

        self.datatoreID = datastoreID
        self.datastoreType = datastoreType
        self.isSrcSys = isSrcSys
