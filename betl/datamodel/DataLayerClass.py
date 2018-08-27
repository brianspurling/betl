from betl.logger import Logger
from betl.logger import alerts
from .DatasetClass import Dataset
from .TableClass import Table
from betl.defaultdataflows import dmDate
from betl.defaultdataflows import dmAudit
from betl import betlConfig

import ast
import os


class DataLayer():

    SCHEMA_DESC_FILE_PREFIX = '/dbSchemaDesc_'

    def __init__(self, conf, dataLayerID):

        self.log = Logger()

        self.conf = conf
        self.databaseID = betlConfig.dataLayers[dataLayerID]
        self.dataLayerID = dataLayerID
        self.datastore = conf.DATA.getDWHDatastore(self.databaseID)
        self.datasets = {}

        schemaDesc = self.getSchemaDescForDataLayer()

        # It's possible we have no schema description for this datalayer
        if schemaDesc is not None:

            for datasetID in schemaDesc['datasetSchemas']:

                self.datasets[datasetID] = Dataset(
                    dataConf=self.conf.DATA,
                    datasetSchemaDesc=schemaDesc['datasetSchemas'][datasetID],
                    datastore=self.datastore,
                    dataLayerID=self.dataLayerID)

            if self.dataLayerID == 'BSE':

                # We also need to create the "default" components of the target
                # model

                if conf.SCHEDULE.DEFAULT_DM_DATE:
                    self.datasets['BSE'].tables['dm_date'] = \
                        Table(self.conf.DATA,
                              dmDate.getSchemaDescription(),
                              self.datastore,
                              dataLayerID='BSE')

                if conf.SCHEDULE.DEFAULT_DM_AUDIT:
                    self.datasets['BSE'].tables['dm_audit'] = \
                        Table(self.conf.DATA,
                              dmAudit.getSchemaDescription(),
                              self.datastore,
                              dataLayerID='BSE')

    def getSchemaDescForDataLayer(self):

        filePath = (self.conf.CTRL.SCHEMA_PATH +
                    DataLayer.SCHEMA_DESC_FILE_PREFIX +
                    self.databaseID + '.txt')

        if os.path.exists(filePath):
            dbSchemaFile = open(filePath, 'r')
            fileContent = dbSchemaFile.read()
            dbSchemaDesc = ast.literal_eval(fileContent)
        else:
            dbSchemaDesc = None

        if dbSchemaDesc is None or self.dataLayerID not in dbSchemaDesc:

            alert = 'Did not find a schema description for datalayer ' + \
                    self.dataLayerID + ' in the ' + self.databaseID + \
                    ' database schema file'

            alerts.logAlert(self.conf, alert)

            return None

        else:

            # For our EXT layer we have the usual schemaDesc, PLUS we will have
            # a mapping of SRC table names to EXT table names
            if self.dataLayerID == 'EXT':

                filePath = self.conf.CTRL.SCHEMA_PATH + '/tableNameMapping.txt'

                if not os.path.exists(filePath):
                    self.conf.DATA.populateSrcTableMap(
                        self.conf.DATA.readSrcSystemSchemas())

                mapFile = open(filePath, 'r')
                tableNameMap = ast.literal_eval(mapFile.read())

                for datasetID in dbSchemaDesc[self.dataLayerID]['datasetSchemas']:
                    for tableName in dbSchemaDesc[self.dataLayerID]['datasetSchemas'][datasetID]['tableSchemas']:
                        dbSchemaDesc[self.dataLayerID]['datasetSchemas'][datasetID]['tableSchemas'][tableName]['srcTableName'] = \
                            tableNameMap[datasetID][tableName]

        return dbSchemaDesc[self.dataLayerID]

    def buildPhysicalSchema(self):

        self.dropPhysicalSchema()

        createStatements = self.getSqlCreateStatements()

        dbCursor = self.datastore.cursor()
        for createStatement in createStatements:
            dbCursor.execute(createStatement)
            self.datastore.commit()

        self.log.logRebuildingPhysicalDataModel(self.dataLayerID)

    def dropPhysicalSchema(self):

        dropStatements = self.getSqlDropStatements()

        dbCursor = self.datastore.cursor()
        for dropStatement in dropStatements:
            dbCursor.execute(dropStatement)
            self.datastore.commit()

    def getSqlCreateStatements(self):
        sqlStatements = []

        for datasetID in self.datasets:
            sqlStatements.extend(
                self.datasets[datasetID].getSqlCreateStatements())
        return sqlStatements

    def getSqlDropStatements(self):
        sqlStatements = []
        for datasetID in self.datasets:
            sqlStatements.extend(
                self.datasets[datasetID].getSqlDropStatements())
        return sqlStatements

    def getListOfTables(self):
        tables = []
        for datasetID in self.datasets:
            tables.extend(self.datasets[datasetID].getListOfTables())
        return tables

    def getColumnsForTable(self, tableName):
        if self.datasets is not None:
            for datasetID in self.datasets:
                c = self.datasets[datasetID].getColumnsForTable(tableName)
                if c is not None:
                    return c
        else:
            # It's possible for there to be no schema desc for a data layer
            return None

    def __str__(self):
        string = ('\n' + '*** Data Layer: ' +
                  self.dataLayerID + ' ***' + '\n')
        for datasetID in self.datasets:
            string += str(self.datasets[datasetID])
        return string
