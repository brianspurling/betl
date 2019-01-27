from .DatasetClass import Dataset
from .TableClass import Table
from betl.defaultdataflows import dmDate
from betl.defaultdataflows import dmAudit
import ast
import os


class DataLayer():

    SCHEMA_DESC_FILE_PREFIX = '/dbSchemaDesc_'

    def __init__(self, conf, dataLayerID):

        self.CONF = conf
        self.databaseID = self.CONF.dataLayers[dataLayerID]
        self.dataLayerID = dataLayerID
        self.datasets = {}

        # We hold a datastore object here, but datstore objects require
        # connections to dbs and logging, which means we don't want to do it
        # on init because airflow will pick it up when processing DAGs
        self.datastore = None

        schemaDesc = self.getDataLayerSchemaDescFromTextFile()

        # It's possible we have no schema description for this datalayer
        if schemaDesc is not None:

            for datasetID in schemaDesc['datasetSchemas']:

                self.datasets[datasetID] = Dataset(
                    conf=self.CONF,
                    datasetSchemaDesc=schemaDesc['datasetSchemas'][datasetID],
                    dataLayerID=self.dataLayerID)

            if self.dataLayerID == 'BSE':

                # We also need to create the "default" components of the target
                # model

                if self.CONF.DEFAULT_DM_DATE:
                    self.datasets['BSE'].tables['dm_date'] = \
                        Table(conf,
                              dmDate.getSchemaDescription(),
                              dataLayerID='BSE')

                if self.CONF.DEFAULT_DM_AUDIT:
                    self.datasets['BSE'].tables['dm_audit'] = \
                        Table(conf,
                              dmAudit.getSchemaDescription(),
                              dataLayerID='BSE')

    def getDatastore(self):
        if self.datastore is None:
            self.datastore = self.CONF.getDWHDatastore(self.databaseID)
        return self.datastore

    def getDataLayerSchemaDescFromTextFile(self):

        filePath = (self.CONF.SCHEMA_PATH +
                    DataLayer.SCHEMA_DESC_FILE_PREFIX +
                    self.databaseID + '.txt')

        if os.path.exists(filePath):
            dbSchemaFile = open(filePath, 'r')
            fileContent = dbSchemaFile.read()
            dbSchemaDesc = ast.literal_eval(fileContent)
        else:
            dbSchemaDesc = None

        if dbSchemaDesc is None or self.dataLayerID not in dbSchemaDesc:

            print('Did not find a schema description for datalayer ' +
                  self.dataLayerID + ' in the ' + self.databaseID +
                  ' database schema file. Use admin CLI to generate.')

            return None

        else:

            # For our EXT layer we have the usual schemaDesc, PLUS we will have
            # a mapping of SRC table names to EXT table names
            if self.dataLayerID == 'EXT':

                filePath = self.CONF.SCHEMA_PATH + '/srcTableNameMapping.txt'

                mapFile = open(filePath, 'r')
                tableNameMap = ast.literal_eval(mapFile.read())

                dl = dbSchemaDesc[self.dataLayerID]
                for datasetID in dl['datasetSchemas']:
                    ds = dl['datasetSchemas'][datasetID]
                    for tableName in ds['tableSchemas']:
                        ds['tableSchemas'][tableName]['srcTableName'] = \
                            tableNameMap[datasetID][tableName]

        return dbSchemaDesc[self.dataLayerID]

    def buildPhysicalSchema(self):

        self.dropPhysicalSchema()

        createStatements = self.getSqlCreateStatements()

        if self.datastore is None:
            self.datastore = self.CONF.getDWHDatastore(self.databaseID)

        dbCursor = self.datastore.cursor()
        for createStatement in createStatements:
            dbCursor.execute(createStatement)
            self.datastore.commit()

        self.CONF.log(
            'logBuildingPhysicalSchema',
            dataLayerID=self.dataLayerID)

    def dropPhysicalSchema(self):

        dropStatements = self.getSqlDropStatements()

        if self.datastore is None:
            self.datastore = self.CONF.getDWHDatastore(self.databaseID)

        dbCursor = self.datastore.cursor()
        for dropStatement in dropStatements:
            dbCursor.execute(dropStatement)
            self.datastore.commit()

    def getSqlCreateStatements(self):
        sqlStatements = []

        if self.datastore is None:
            self.datastore = self.CONF.getDWHDatastore(self.databaseID)

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
