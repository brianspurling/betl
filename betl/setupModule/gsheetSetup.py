import gspread
from oauth2client.service_account import ServiceAccountCredentials

from betl.defaultdataflows import dmDate


def connectToGSheets(self):
    self.GSPREAD = gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_name(
            self.GOOGLE_API_KEY_FILENAME,
            self.GOOGLE_API_SCOPE))


def createSchemaDescGSheets(self, response):
    # TODO: error if titles already exist? and below.
    if response.lower() in ['y', '']:

        if self.GSPREAD is None:
            self.connectToGSheets()

        etl = self.GSPREAD.create(self.SCHEMA_DESC_ETL_GSHEET_TITLE)
        etl.share(
            self.GOOGLE_ACCOUNT,
            perm_type='user',
            role='writer')

        trg = self.GSPREAD.create(self.SCHEMA_DESC_TRG_GSHEET_TITLE)
        trg.share(
            self.GOOGLE_ACCOUNT,
            perm_type='user',
            role='writer')


def createMDMGsheet(self, response):
    if response.lower() in ['y', '']:

        if self.GSPREAD is None:
            self.connectToGSheets()

        mdm = self.GSPREAD.create(self.MDM_GHSEET_TITLE)
        mdm.share(
            self.GOOGLE_ACCOUNT,
            perm_type='user',
            role='writer')


def createDefaultRowsGsheet(self, response):
    if response.lower() in ['y', '']:

        if self.GSPREAD is None:
            self.connectToGSheets()

        dr = self.GSPREAD.create(self.DEFAULT_ROWS_GHSEET_TITLE)
        data = dmDate.getDefaultRows()
        colHeadings = list(data[0].keys())
        ws = dr.get_worksheet(0)

        cells = ws.range(1, 1, len(data)+1, len(data[0]))

        count = 0
        for colHeading in colHeadings:
            cells[count].value = colHeading
            count += 1

        for row in data:
            for colName in row:
                cells[count].value = row[colName]
                count += 1

        ws.update_cells(cells)
        ws.update_title('dm_date')
        dr.share(
            self.GOOGLE_ACCOUNT,
            perm_type='user',
            role='writer')
