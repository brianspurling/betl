import pandas as pd
import os
import ast
import json

from betl.logger import logger
from betl.logger import alerts
from betl import betlConfig

from betl.io import fileIO

from betl.io import PostgresDatastore
from betl.io import SqliteDatastore
from betl.io import GsheetDatastore
from betl.io import ExcelDatastore
from betl.io import FileDatastore

from betl.datamodel import SrcDataLayer
from betl.datamodel import StgDataLayer
from betl.datamodel import TrgDataLayer
from betl.datamodel import SumDataLayer


class Data():

    def __init__(self, conf):

        self.CONF = conf

        self.DATABASES = betlConfig.databases
        self.DATA_LAYERS = betlConfig.dataLayers
        self.AUDIT_COLS = pd.DataFrame(betlConfig.auditColumns)
        self.SRC_SYSTEM_LIST = []

        for srcSysID in self.CONF.allConfig['data']['src_sys']:
            self.SRC_SYSTEM_LIST.append(srcSysID)

        self.GOOGLE_API_SCOPE = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
        # The following are either datastore(s) or require datastores to
        # initialise them. Therefore we init these as-and-when we need them,
        # to avoid long delays at the start of every execution
        self.SCHEMA_DESCRIPTION_GSHEETS = {}
        self.LOGICAL_DATA_MODELS = {}
        self.DWH_DATABASES = {}
        self.DEFAULT_ROW_SRC = None
        self.MDM_SRC = None
        self.SRC_SYSTEMS = {}

        # We'll need this later, when we connect to the various Google Sheets
        self.apiKey = \
            self.CONF.allConfig['data']['GSHEETS_API_KEY_FILE']

    def getSchemaDescGSheetDatastore(self, dbID):
        if dbID in self.SCHEMA_DESCRIPTION_GSHEETS:
            return self.SCHEMA_DESCRIPTION_GSHEETS[dbID]
        else:
            fname = dbID + '_FILENAME'
            self.SCHEMA_DESCRIPTION_GSHEETS[dbID] = \
                GsheetDatastore(
                    ssID=dbID,
                    apiScope=self.GOOGLE_API_SCOPE,
                    apiKey=self.apiKey,
                    filename=self.CONF.allConfig['data']['schema_descs'][fname],
                    isSchemaDesc=True)
            return self.SCHEMA_DESCRIPTION_GSHEETS[dbID]

    def getDataLayerLogicalSchema(self, dataLayerID):
        if dataLayerID in self.LOGICAL_DATA_MODELS:
            return self.LOGICAL_DATA_MODELS[dataLayerID]
        else:
            if dataLayerID == 'SRC':
                self.LOGICAL_DATA_MODELS['SRC'] = SrcDataLayer(self.CONF)
            elif dataLayerID == 'STG':
                self.LOGICAL_DATA_MODELS['STG'] = StgDataLayer(self.CONF)
            elif dataLayerID == 'TRG':
                self.LOGICAL_DATA_MODELS['TRG'] = TrgDataLayer(self.CONF)
            elif dataLayerID == 'SUM':
                self.LOGICAL_DATA_MODELS['SUM'] = SumDataLayer(self.CONF)
            return self.LOGICAL_DATA_MODELS[dataLayerID]

    def getDWHDatastore(self, dbID):
        if dbID in self.DWH_DATABASES:
            return self.DWH_DATABASES[dbID]
        else:
            self.DWH_DATABASES[dbID] = \
                PostgresDatastore(
                    dbID=dbID,
                    host=self.CONF.allConfig['data']['dwh_dbs'][dbID]['HOST'],
                    dbName=self.CONF.allConfig['data']['dwh_dbs'][dbID]['DBNAME'],
                    user=self.CONF.allConfig['data']['dwh_dbs'][dbID]['USER'],
                    password=self.CONF.allConfig['data']['dwh_dbs'][dbID]['PASSWORD'],
                    createIfNotFound=True)
            return self.DWH_DATABASES[dbID]

    def getDefaultRowsDatastore(self):
        if self.DEFAULT_ROW_SRC is not None:
            return self.DEFAULT_ROW_SRC
        else:
            apiKy = self.CONF.allConfig['data']['GSHEETS_API_KEY_FILE']
            fname = self.CONF.allConfig['data']['default_rows']['FILENAME']
            self.DEFAULT_ROW_SRC = \
                GsheetDatastore(
                    ssID='DR',
                    apiScope=self.GOOGLE_API_SCOPE,
                    apiKey=apiKy,
                    filename=fname)
            return self.DEFAULT_ROW_SRC

    def getMDMDatastore(self):
        if self.MDM_SRC is not None:
            return self.MDM_SRC
        else:
            self.MDM_SRC = \
                GsheetDatastore(
                    ssID='MDM',
                    apiScope=self.GOOGLE_API_SCOPE,
                    apiKey=self.CONF.allConfig['data']['GSHEETS_API_KEY_FILE'],
                    filename=self.CONF.allConfig['data']['mdm']['FILENAME'])
            return self.MDM_SRC

    def getSrcSysDatastore(self, ssID):
        if ssID in self.SRC_SYSTEMS:
            return self.SRC_SYSTEMS[ssID]
        else:

            logger.logInitialiseSrcSysDatastore(
                datastoreID=ssID,
                datastoreType=self.CONF.allConfig['data']['src_sys'][ssID]['TYPE'])

            if self.CONF.allConfig['data']['src_sys'][ssID]['TYPE'] == 'POSTGRES':
                pw = self.CONF.allConfig['data']['src_sys'][ssID]['PASSWORD']
                self.SRC_SYSTEMS[ssID] = \
                    PostgresDatastore(
                        dbID=ssID,
                        host=self.CONF.allConfig['data']['src_sys'][ssID]['HOST'],
                        dbName=self.CONF.allConfig['data']['src_sys'][ssID]['DBNAME'],
                        user=self.CONF.allConfig['data']['src_sys'][ssID]['USER'],
                        password=pw,
                        isSrcSys=True)

            elif self.CONF.allConfig['data']['src_sys'][ssID]['TYPE'] == 'SQLITE':
                fname = self.CONF.allConfig['data']['src_sys'][ssID]['FILENAME']
                self.SRC_SYSTEMS[ssID] = \
                    SqliteDatastore(
                        dbID=ssID,
                        path=self.CONF.allConfig['data']['src_sys'][ssID]['PATH'],
                        filename=fname,
                        isSrcSys=True)

            elif self.CONF.allConfig['data']['src_sys'][ssID]['TYPE'] == 'FILESYSTEM':
                fileExt = self.CONF.allConfig['data']['src_sys'][ssID]['FILE_EXT']
                delim = self.CONF.allConfig['data']['src_sys'][ssID]['DELIMITER']
                quoteChar = self.CONF.allConfig['data']['src_sys'][ssID]['QUOTECHAR']
                self.SRC_SYSTEMS[ssID] = \
                    FileDatastore(
                        fileSysID=ssID,
                        path=self.CONF.allConfig['data']['src_sys'][ssID]['PATH'],
                        fileExt=fileExt,
                        delim=delim,
                        quotechar=quoteChar,
                        isSrcSys=True)

            elif self.CONF.allConfig['data']['src_sys'][ssID]['TYPE'] == 'GSHEET':
                apiKey = self.CONF.allConfig['data']['GSHEETS_API_KEY_FILE']
                self.SRC_SYSTEMS[ssID] = \
                    GsheetDatastore(
                        ssID=ssID,
                        apiScope=self.GOOGLE_API_SCOPE,
                        apiKey=apiKey,
                        filename=self.CONF.allConfig['data']['src_sys'][ssID]['FILENAME'],
                        isSrcSys=True)

            elif self.CONF.allConfig['data']['src_sys'][ssID]['TYPE'] == 'EXCEL':
                self.SRC_SYSTEMS[ssID] = \
                    ExcelDatastore(
                        ssID=ssID,
                        path=self.CONF.allConfig['data']['src_sys'][ssID]['PATH'],
                        filename=self.CONF.allConfig['data']['src_sys'][ssID]['FILENAME'] +
                        self.CONF.allConfig['data']['src_sys'][ssID]['FILE_EXT'],
                        isSrcSys=True)

            return self.SRC_SYSTEMS[ssID]

    def refreshSchemaDescsFromGsheets(self):

        # Get the schema descriptions from schemas/, or from Google Sheets, if
        # the sheets have been edited since they were last saved to csv

        logger.logCheckLastModTimeOfSchemaDescGSheet()
        # Get the last modified dates of the versions saved to csv
        modTimesFile = open(self.CONF.CTRL.SCHEMA_PATH + '/lastModifiedTimes.txt', 'r+')
        fileContent = modTimesFile.read()
        if fileContent == '':
            lastModifiedTimes = {}
        else:
            lastModifiedTimes = ast.literal_eval(fileContent)
        oneOrMoreLastModTimesChanged = False
        lastModTimesChanged = {}

        # Check the last modified time of the Google Sheets
        for dbID in self.DATABASES:
            gSheet = self.getSchemaDescGSheetDatastore(dbID)
            if (gSheet.filename not in lastModifiedTimes
               or gSheet.getLastModifiedTime() !=
               lastModifiedTimes[gSheet.filename]):
                oneOrMoreLastModTimesChanged = True
                lastModTimesChanged[dbID] = True

        if oneOrMoreLastModTimesChanged:
            logger.logRefreshingSchemaDescsFromGsheets(
                len(lastModTimesChanged))
            for dbID in lastModTimesChanged:
                gSheet = self.getSchemaDescGSheetDatastore(dbID)
                self.refreshSchemaDescCSVs(gSheet, dbID)
                lastModifiedTimes[gSheet.filename] = \
                    gSheet.getLastModifiedTime()
            logger.logRefreshingSchemaDescsFromGsheets_done()

        if len(lastModTimesChanged) > 0:
            modTimesFile = open(self.CONF.CTRL.SCHEMA_PATH + '/lastModifiedTimes.txt', 'w')
            modTimesFile.write(json.dumps(lastModifiedTimes))

    def refreshSchemaDescCSVs(self, datastore, dbID):

        # Start by builing an array of all the releavnt worksheets from the
        # DB's schema desc gsheet.
        worksheets = []
        # It's important to call getWorksheets() again, rather than the
        # worksheets attribute, because we might have just replaced the SRC
        # worksheets (if we ran READ_SRC)
        # TODO: in fact, I'm increasingly writing my conf class methods to
        # check whether the id in question exists and handle accordingly,
        # I should probably work through all calls to conf and make them
        # consistently use these methods
        for gWorksheetTitle in datastore.getWorksheets():
            # skip any sheets that aren't prefixed with the DB (e.g. ETL or
            # TRG)
            if gWorksheetTitle[0:4] == dbID + '.':
                worksheets.append(datastore.worksheets[gWorksheetTitle])

        dbSchemaDesc = {}

        logger.logLoadingDBSchemaDescsFromGsheets(dbID)

        for ws in worksheets:
            # Get the dataLayer, dataModel and table name from the worksheet
            # title
            dataLayerID = ws.title[ws.title.find('.')+1:ws.title.rfind('.')]
            dataLayerID = dataLayerID[:dataLayerID.rfind('.')]
            dataModelID = ws.title[ws.title.find('.')+1:ws.title.rfind('.')]
            dataModelID = dataModelID[dataModelID.find('.')+1:]
            tableName = ws.title[ws.title.rfind('.')+1:]

            # If needed, create a new item in our db schema desc for
            # this data layer, and a new item in our dl schema desc for
            # this data model.
            # (there is a worksheet per table, many tables per data model,
            # and many data models per database)
            if dataLayerID not in dbSchemaDesc:
                dbSchemaDesc[dataLayerID] = {
                    'dataLayerID': dataLayerID,
                    'dataModelSchemas': {}
                }
            dlSchemaDesc = dbSchemaDesc[dataLayerID]
            if dataModelID not in dlSchemaDesc['dataModelSchemas']:
                dlSchemaDesc['dataModelSchemas'][dataModelID] = {
                    'dataModelID': dataModelID,
                    'tableSchemas': {}
                }
            dmSchemaDesc = dlSchemaDesc['dataModelSchemas'][dataModelID]

            # Create a new table schema description
            tableSchema = {
                'tableName': tableName,
                'columnSchemas': {}
            }

            # Pull out the column schema descriptions from the Google
            # worksheeet and restructure a little
            colSchemaDescsFromWS = ws.get_all_records()
            for colSchemaDescFromWS in colSchemaDescsFromWS:
                colName = colSchemaDescFromWS['Column Name']
                fkDimension = 'None'
                if 'FK Dimension' in colSchemaDescFromWS:
                    fkDimension = colSchemaDescFromWS['FK Dimension']

                # append this column schema desc to our tableSchema object
                tableSchema['columnSchemas'][colName] = {
                    'tableName':   tableName,
                    'columnName':  colName,
                    'dataType':    colSchemaDescFromWS['Data Type'],
                    'columnType':  colSchemaDescFromWS['Column Type'],
                    'fkDimension': fkDimension
                }

            # Finally, add the tableSchema to our data dataModel schema desc
            dmSchemaDesc['tableSchemas'][tableName] = tableSchema

        with open(self.CONF.CTRL.SCHEMA_PATH + '/dbSchemaDesc_' + dbID + '.txt', 'w') as file:
            file.write(json.dumps(dbSchemaDesc))

    # TODO: this is a HORRIBLE mess of code and needs heavy refactoring!
    def autoPopulateSrcSchemaDescriptions(self):

        logger.logAutoPopSchemaDescsFromSrcStart()

        # First, loop through the ETL DB schema desc spreadsheet and delete
        # any worksheets prefixed ETL.SRC.

        logger.logDeleteSrcSchemaDescWsFromSS()
        ss = self.getSchemaDescGSheetDatastore('ETL').conn

        for ws in ss.worksheets():
            if ws.title.find('ETL.SRC.') == 0:
                ss.del_worksheet(ws)

        # Each source system will create a new data model within our SRC data
        # layer (within our ETL database)
        srcSysSchemas = {}
        srcTableMap = {}
        for srcSysID in self.SRC_SYSTEM_LIST:

            srcSysDS = self.getSrcSysDatastore(srcSysID)

            # The object we're going to build up before writing to the GSheet
            srcSysSchemas[srcSysID] = {
                'dataModelID': srcSysID,
                'tableSchemas': {}
            }

            if srcSysDS.datastoreType == 'POSTGRES':
                # one row in information_schema.columns for each column,
                # spanning multiple tables
                dbCursor = srcSysDS.conn.cursor()
                dbCursor.execute(
                    "SELECT * FROM information_schema.columns c " +
                    "WHERE c.table_schema = 'public' " +
                    "ORDER BY c.table_name")
                postgresSchema = dbCursor.fetchall()
                previousTableName = ''
                colSchemasFromPG = []
                for colSchemaFromPG in postgresSchema:
                    currentTableName = ('src_' + srcSysID + '_' +
                                        colSchemaFromPG[2])
                    if currentTableName == previousTableName:
                        colSchemasFromPG.append(colSchemaFromPG)
                    else:
                        colSchemas = {}

                        for colSchemaFromPG in colSchemasFromPG:
                            colName = colSchemaFromPG[3]

                            colSchemas[colName] = {
                                'tableName': previousTableName,
                                'columnName': colName,
                                'dataType': colSchemaFromPG[7],
                                'columnType': 'Attribute',
                                'fkDimension': None
                            }

                        tableSchema = {
                            'tableName': previousTableName,
                            'columnSchemas': colSchemas
                        }
                        tableName = previousTableName
                        srcSysSchemas[srcSysID]['tableSchemas'][tableName] = \
                            tableSchema
                        colSchemasFromPG = []
                        colSchemasFromPG.append(colSchemaFromPG)
                        previousTableName = currentTableName

            elif srcSysDS.datastoreType == 'SQLITE':
                # one row in information_schema.columns for each column,
                # spanning multiple tables
                dbCursor = srcSysDS.conn.cursor()
                dbCursor.execute("SELECT * FROM sqlite_master c " +
                                 "WHERE type = 'table' ")
                tables = dbCursor.fetchall()
                for table in tables:
                    colSchemas = {}
                    for row in srcSysDS.conn.execute(
                         "pragma table_info('" + table[1] + "')").fetchall():

                        colSchemas[row[1]] = {
                            'tableName': table[1],
                            'columnName': row[1],
                            'dataType': row[2],
                            'columnType': 'Attribute',
                            'fkDimension': None
                        }

                    tableSchema = {
                        'tableName': table[1],
                        'columnSchemas': colSchemas
                    }

                    srcSysSchemas[srcSysID]['tableSchemas'][table[1]] = \
                        tableSchema

            elif (srcSysDS.datastoreType == 'FILESYSTEM' and
                  srcSysDS.fileExt == '.csv'):
                # one src filesystem will contain 1+ files. Each file is a
                # table, obviously, and each will have a list of cols in the
                # first row. Get all the files (in the root dir), then loop
                # through each one
                files = []
                for (dirpath, dirnames, filenames) in os.walk(srcSysDS.path):
                    files.extend(filenames)
                    break  # Just the root
                for filename in files:

                    if filename.find('.csv') > 0:
                        df = fileIO.readDataFromCsv(
                            fileNameMap=None,
                            path=srcSysDS.path,
                            filename=filename,
                            sep=srcSysDS.delim,
                            quotechar=srcSysDS.quotechar,
                            isTmpData=False,
                            getFirstRow=True)

                        cleanFName = filename[0:len(filename)-4]

                        colSchemas = {}

                        for colName in df:
                            colSchemas[colName] = {
                                'tableName': cleanFName,
                                'columnName': colName,
                                'dataType': 'TEXT',
                                'columnType': 'Attribute',
                                'fkDimension': None
                            }

                        tableSchema = {
                            'tableName': cleanFName,
                            'columnSchemas': colSchemas
                        }

                        srcSysSchemas[srcSysID]['tableSchemas'][cleanFName] = \
                            tableSchema

            elif (srcSysDS.datastoreType == 'GSHEET'):
                # one spreadsheet will contain multiple worksheets, each
                # worksheet containing one table. The top row is the column
                # headings.
                worksheets = srcSysDS.getWorksheets()
                for wsName in worksheets:
                    colHeaders = worksheets[wsName].row_values(1)
                    colSchemas = {}
                    for colName in colHeaders:
                        if colName != '':
                            colSchemas[colName] = {
                                'tableName': wsName,
                                'columnName': colName,
                                'dataType': 'TEXT',
                                'columnType': 'Attribute',
                                'fkDimension': None
                            }

                    tableSchema = {
                        'tableName': wsName,
                        'columnSchemas': colSchemas
                    }
                    srcSysSchemas[srcSysID]['tableSchemas'][wsName] = \
                        tableSchema

            elif (srcSysDS.datastoreType == 'EXCEL'):
                # one spreadsheet will contain multiple worksheets, each
                # worksheet containing one table. The top row is the column
                # headings.
                worksheets = srcSysDS.getWorksheets()
                for wsName in worksheets:
                    colHeaders = worksheets[wsName]['1:1']
                    colSchemas = {}
                    for cell in colHeaders:
                        colName = cell.value
                        if colName != '' and colName is not None:
                            colSchemas[colName] = {
                                'tableName': wsName,
                                'columnName': colName,
                                'dataType': 'TEXT',
                                'columnType': 'Attribute',
                                'fkDimension': None
                            }
                        else:
                            break

                    tableSchema = {
                        'tableName': wsName,
                        'columnSchemas': colSchemas
                    }
                    srcSysSchemas[srcSysID]['tableSchemas'][wsName] = \
                        tableSchema

            else:
                raise ValueError(
                    "Failed to auto-populate SRC Layer " +
                    "schema desc: Source system type is " +
                    srcSysDS.datastoreType +
                    ". Stopping execution. We only " +
                    "deal with 'POSTGRES', 'FILESYSTEM' & 'GSHEET' " +
                    " source system types, so cannot auto-populate " +
                    "the ETL.SRC schemas for this source system")

            # Check we managed to find some kind of schema from the source
            # system
            if (len(srcSysSchemas[srcSysID]['tableSchemas']) == 0):
                raise ValueError(
                    "Failed to auto-populate SRC Layer schema desc:" +
                    " we could not find any meta data in the src " +
                    "system with which to construct a schema " +
                    "description")
            else:

                # Some data sources can provide us with table names
                # incompatible with Postgres (e.g. worksheet names in
                # Excel/GSheets). So we will create a mapping of actual names
                # to postgres names, to be used whenever we need to pull data
                # out of source. For simplicity, we'll do this for
                # all sources, even though some will always be the same.

                srcTableMap[srcSysID] = {}

                tableSchemas = srcSysSchemas[srcSysID]['tableSchemas']
                for tableName_src in tableSchemas:

                    tableName = self.cleanTableName(tableName_src)
                    srcTableMap[srcSysID][tableName] = tableName_src

                    colSchemas = \
                        tableSchemas[tableName_src]['columnSchemas']

                    wsName = 'ETL.SRC.' + srcSysID + '.' + 'src_' + \
                        srcSysID.lower() + '_' + tableName

                    ws = ss.add_worksheet(title=wsName,
                                          rows=len(colSchemas)+1,
                                          cols=3)

                    # We build up our new GSheets table first, in memory,
                    # then write it all in one go.
                    cell_list = ws.range('A1:C'+str(len(colSchemas)+1))
                    rangeRowCount = 0
                    cell_list[rangeRowCount].value = 'Column Name'
                    cell_list[rangeRowCount+1].value = 'Data Type'
                    cell_list[rangeRowCount+2].value = 'Column Type'
                    rangeRowCount += 3
                    for col in colSchemas:
                        cell_list[rangeRowCount].value = \
                            colSchemas[col]['columnName']
                        cell_list[rangeRowCount+1].value = \
                            colSchemas[col]['dataType']
                        cell_list[rangeRowCount+2].value = \
                            colSchemas[col]['columnType']
                        rangeRowCount += 3

                    ws.update_cells(cell_list)

        with open(self.CONF.CTRL.SCHEMA_PATH + '/tableNameMapping.txt', 'w+') as file:
            file.write(json.dumps(srcTableMap))

        logger.logAutoPopSchemaDescsFromSrcFinish()

    def cleanTableName(self, tableName_src):
        tableName = tableName_src
        tableName = tableName.replace(' ', '_')
        tableName = tableName.replace('(', '')
        tableName = tableName.replace(')', '')
        tableName = tableName.replace('-', '')
        tableName = tableName.lower()
        return tableName

    def checkDBsForSuperflousTables(self, conf):
        query = ("SELECT table_name FROM information_schema.tables " +
                 "WHERE table_schema = 'public'")

        etlDBCursor = self.getDWHDatastore('ETL').cursor()
        etlDBCursor.execute(query)
        trgDBCursor = self.getDWHDatastore('TRG').cursor()
        trgDBCursor.execute(query)

        # query returns list of tuples, (<tablename>, )
        allTables = []
        allTables.extend([item[0] for item in etlDBCursor.fetchall()])
        allTables.extend([item[0] for item in trgDBCursor.fetchall()])

        dataModelTables = []
        for dataLayerID in self.DATA_LAYERS:
            dataModelTables.extend(
                self.getDataLayerLogicalSchema(dataLayerID).getListOfTables())

        superflousTableNames = []
        for tableName in allTables:
            if tableName not in dataModelTables:
                superflousTableNames.append(tableName)

        if len(superflousTableNames) > 0:
            op = ''
            op += 'The following tables were found in one of the '
            op += 'databases but not in the logical data model. \n'
            op += 'They should be checked and removed. \n'
            op += '\n'
            op += '  ' + ',\n  '.join(superflousTableNames)

            alerts.logAlert(conf, op)
