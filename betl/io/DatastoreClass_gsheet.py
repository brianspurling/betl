from .DatastoreClass import Datastore

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import httplib2


class GsheetDatastore(Datastore):

    def __init__(self,
                 ssID,
                 apiScope,
                 apiKey,
                 filename,
                 isSrcSys=False,
                 isSchemaDesc=False):

        Datastore.__init__(self,
                           datastoreID=ssID,
                           datastoreType='GSHEET',
                           isSrcSys=isSrcSys,
                           isSchemaDesc=isSchemaDesc)

        # TODO don't do all this on init, it slows down the start
        # of the job, which has a diproportionate effect on dev time.
        self.ssID = ssID
        self.apiScope = apiScope
        self.apiKey = apiKey
        self.filename = filename
        self.conn = self.getGsheetConnection()
        self.worksheets = self.getWorksheets()
        self.gdriveConn = self.getGdriveConnection()
        self.lastModifiedTime = self.getLastModifiedTime()

    def getGsheetConnection(self):
        _client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                self.apiKey,
                self.apiScope))
        return _client.open(self.filename)

    def getGdriveConnection(self):
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.apiKey,
            'https://www.googleapis.com/auth/drive.metadata.readonly')
        return build('drive', 'v3', http=creds.authorize(httplib2.Http()))

    def getWorksheets(self):
        worksheets = {}
        for ws in self.conn.worksheets():
            worksheets[ws.title] = ws
        self.worksheets = worksheets
        return worksheets

    def getLastModifiedTime(self):
        result = self.gdriveConn.files().get(
            fileId=self.conn.id,
            fields='modifiedTime').execute()
        return result['modifiedTime']

    def __str__(self):
        string = ('\n\n' + '*** Datastore: ' +
                  self.ssID + ' (' + self.datastoreType + ', ' +
                  self.filename + ')' + ' ***' '\n')
        string += '  - worksheets: ' + '\n'
        for wsTitle in self.worksheets:
            string += '    - ' + wsTitle + '\n'
        return string
