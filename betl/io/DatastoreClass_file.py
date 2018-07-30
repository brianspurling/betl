from .DatastoreClass import Datastore


class FileDatastore(Datastore):

    def __init__(self, fileSysID, path, fileExt, delim, quotechar,
                 isSrcSys=False):

        Datastore.__init__(self,
                           datastoreID=fileSysID,
                           datastoreType='FILESYSTEM',
                           isSrcSys=isSrcSys)

        self.fileSysID = fileSysID
        self.path = path
        self.fileExt = fileExt
        self.delim = delim
        self.quotechar = quotechar
