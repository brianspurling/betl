from openpyxl import load_workbook
from .DatastoreClass import Datastore


class ExcelDatastore(Datastore):

    def __init__(self,
                 ssID,
                 path,
                 filename,
                 isSrcSys=False,
                 isSchemaDesc=False):

        Datastore.__init__(self,
                           datastoreID=ssID,
                           datastoreType='EXCEL',
                           isSrcSys=isSrcSys,
                           isSchemaDesc=isSchemaDesc)

        self.ssID = ssID
        self.path = path
        self.filename = filename
        self.conn = self.getExcelConnection()
        self.worksheets = self.getWorksheets()

    def getExcelConnection(self):
        return load_workbook(filename=self.path + self.filename)

    def getWorksheets(self):

        worksheets = {}
        for ws in self.conn.worksheets:
            worksheets[ws.title] = ws
        self.worksheets = worksheets
        return worksheets

    def __str__(self):
        string = ('\n\n' + '*** Datastore: ' +
                  self.ssID + ' (' + self.datastoreType + ', ' +
                  self.filename + ')' + ' ***' '\n')
        string += '  - worksheets: ' + '\n'
        for wsTitle in self.worksheets:
            string += '    - ' + wsTitle + '\n'
        return string
