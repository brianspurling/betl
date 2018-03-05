from .dataModel import DataModel
from .dataModel import SrcDataModel


class DataLayer():

    def __init__(self, dbID, dataLayerID, conf):

        self.conf = conf
        self.databaseID = dbID
        self.dataLayerID = dataLayerID

        self.datastore = conf.app.DWH_DATABASES[dbID]
        self.schemaDescSpreadsheetDatastore = \
            conf.app.SCHEMA_DESCRIPTION_GSHEETS[dbID]
        self.dataModels = self.buildLogicalDataModels()

    #
    # Logical Data Model (Gsheets)
    #

    def buildLogicalDataModels(self):

        dataModelSchemas = self.getSchemaDescriptionForThisDataLayer()

        dataModels = {}
        for dataModelID in dataModelSchemas:
            if self.dataLayerID == 'SRC':
                dataModels[dataModelID] = \
                    SrcDataModel(dataModelSchemas[dataModelID], self.conf)
            else:
                dataModels[dataModelID] = \
                    DataModel(dataModelSchemas[dataModelID])

        return dataModels

    def getSchemaDescriptionForThisDataLayer(self):

        dbSchemaDescWorksheets = self.schemaDescSpreadsheetDatastore.worksheets

        # One database can have many data layers, so filter down to only the
        # datalayer we're interested in (the code will be in the worksheet
        # title)
        dlSchemaDescWorksheets = []
        for gWorksheetTitle in dbSchemaDescWorksheets:
            if gWorksheetTitle.find('.' + self.dataLayerID + '.') > -1:
                dlSchemaDescWorksheets.append(
                    dbSchemaDescWorksheets[gWorksheetTitle])

        dataLayerSchemaDesc = {}

        for ws in dlSchemaDescWorksheets:

            # Get the datamodel ID and table name from the worksheet title
            dataModelID = ws.title[ws.title.find('.')+1:ws.title.rfind('.')]
            dataModelID = dataModelID[dataModelID.find('.')+1:]
            tableName = ws.title[ws.title.rfind('.')+1:]

            # If needed, create a new item in our dl schema description for
            # this data model (there is a worksheet per table, and many tables
            # per data model)
            if dataModelID not in dataLayerSchemaDesc:
                dataLayerSchemaDesc[dataModelID] = {
                    'dataModelID': dataModelID,
                    'tableSchemas': {}
                }

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
                fkDimension = None
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

            # Finally, add the tableSchema to our data dataLayer schema desc
            dataLayerSchemaDesc[dataModelID]['tableSchemas'][tableName] = \
                tableSchema

        return dataLayerSchemaDesc

    # Physical Data Model (Postgres)

    def buildPhysicalDataModel(self):

        self.dropPhysicalDataModel()

        createStatements = self.getSqlCreateStatements()

        dbCursor = self.datastore.cursor()
        for createStatement in createStatements:
            dbCursor.execute(createStatement)
            self.datastore.commit()

    def dropPhysicalDataModel(self):

        dropStatements = self.getSqlDropStatements()

        dbCursor = self.datastore.cursor()
        for dropStatement in dropStatements:
            dbCursor.execute(dropStatement)
            self.datastore.commit()

    def truncatePhysicalDataModel(self, truncDims=True, truncFacts=True):

        truncateStatements = self.getSqlTruncateStatements(
            truncDims=truncDims,
            truncFacts=truncFacts)

        trgDbCursor = self.datastore.cursor()
        for truncateStatement in truncateStatements:
            trgDbCursor.execute(truncateStatement)
            self.datastore.commit()

    def getSqlCreateStatements(self):
        sqlStatements = []
        for dataModelID in self.dataModels:
            sqlStatements.extend(
                self.dataModels[dataModelID].getSqlCreateStatements())
        return sqlStatements

    def getSqlDropStatements(self):
        sqlStatements = []
        for dataModelID in self.dataModels:
            sqlStatements.extend(
                self.dataModels[dataModelID].getSqlDropStatements())
        return sqlStatements

    def getSqlTruncateStatements(self, truncDims, truncFacts):
        sqlStatements = []
        for dataModelID in self.dataModels:
            sqlStatements.extend(
                self.dataModels[dataModelID].getSqlTruncateStatements(
                    truncDims=truncDims,
                    truncFacts=truncFacts))
        return sqlStatements

    def __str__(self):
        string = ('\n' + '\n' + '*** Data Layer: ' +
                  self.dataLayerID + ' ***' + '\n')
        for dataModelID in self.dataModels:
            string += str(self.dataModels[dataModelID])
        return string


class SrcDataLayer(DataLayer):

    def __init__(self, conf):

        DataLayer.__init__(self,
                           dbID='ETL',
                           dataLayerID='SRC',
                           conf=conf)


class StgDataLayer(DataLayer):

    def __init__(self, conf):

        DataLayer.__init__(self,
                           dbID='ETL',
                           dataLayerID='STG',
                           conf=conf)


class TrgDataLayer(DataLayer):

    def __init__(self, conf):

        DataLayer.__init__(self,
                           dbID='TRG',
                           dataLayerID='TRG',
                           conf=conf)

    def resetSKSequences(self):

        resetStatements = self.getSqlResetSKSequences()

        trgDbCursor = self.datastore.cursor()
        for resetStatement in resetStatements:
            trgDbCursor.execute(resetStatement)
            self.datastore.commit()

    def getSqlResetSKSequences(self):
        sqlStatements = []
        for dataModelID in self.dataModels:
            sqlStatements.extend(
                self.dataModels[dataModelID].getSqlResetPrimaryKeySequences())
        return sqlStatements


class SumDataLayer(DataLayer):

    def __init__(self, conf):

        DataLayer.__init__(self,
                           dbID='TRG',
                           dataLayerID='SUM',
                           conf=conf)
