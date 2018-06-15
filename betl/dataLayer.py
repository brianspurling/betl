from . import logger
from .dataModel import DataModel
from .dataModel import SrcDataModel
from .table import TrgTable
from . import df_dmDate
from . import df_dmAudit

import ast


class DataLayer():

    def __init__(self, dbID, dataLayerID, dataConf):

        self.dataConf = dataConf
        self.databaseID = dbID
        self.dataLayerID = dataLayerID

        self.datastore = dataConf.getDWHDatastore(dbID)
        self.dataModels = self.buildLogicalDataModels()

    def buildLogicalDataModels(self):

        file = open('schemas/dbSchemaDesc_' + self.databaseID + '.txt', 'r')
        dbSchemaDesc = ast.literal_eval(file.read())
        try:
            dlSchemaDesc = dbSchemaDesc[self.dataLayerID]
        except KeyError:
            raise ValueError('Failed to find any schema description for ' +
                             'data layer ' + self.dataLayerID)
        dataModels = {}

        for dataModelID in dlSchemaDesc['dataModelSchemas']:
            if self.dataLayerID == 'SRC':
                # Each dataModel in the SRC dataLayer is a source system
                dataModels[dataModelID] = \
                    SrcDataModel(self.dataConf,
                                 dlSchemaDesc['dataModelSchemas'][dataModelID],
                                 self.datastore,
                                 self.dataLayerID)
            else:
                dataModels[dataModelID] = \
                    DataModel(self.dataConf,
                              dlSchemaDesc['dataModelSchemas'][dataModelID],
                              self.datastore,
                              self.dataLayerID)

        return dataModels

    def buildPhysicalDataModel(self):

        self.dropPhysicalDataModel()

        createStatements = self.getSqlCreateStatements()

        dbCursor = self.datastore.cursor()
        for createStatement in createStatements:
            dbCursor.execute(createStatement)
            self.datastore.commit()

        logger.logRebuildingPhysicalDataModel(self.dataLayerID)

    def dropPhysicalDataModel(self):

        dropStatements = self.getSqlDropStatements()

        dbCursor = self.datastore.cursor()
        for dropStatement in dropStatements:
            dbCursor.execute(dropStatement)
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

    def getListOfTables(self):
        tables = []
        for dataModelID in self.dataModels:
            tables.extend(self.dataModels[dataModelID].getListOfTables())
        return tables

    def getColumnsForTable(self, tableName):
        for dataModelID in self.dataModels:
            c = self.dataModels[dataModelID].getColumnsForTable(tableName)
            if c is not None:
                return c

    def __str__(self):
        string = ('\n' + '*** Data Layer: ' +
                  self.dataLayerID + ' ***' + '\n')
        for dataModelID in self.dataModels:
            string += str(self.dataModels[dataModelID])
        return string


class SrcDataLayer(DataLayer):

    def __init__(self, dataConf):

        DataLayer.__init__(self,
                           dbID='ETL',
                           dataLayerID='SRC',
                           dataConf=dataConf)


class StgDataLayer(DataLayer):

    def __init__(self, dataConf):

        DataLayer.__init__(self,
                           dbID='ETL',
                           dataLayerID='STG',
                           dataConf=dataConf)


class TrgDataLayer(DataLayer):

    def __init__(self, dataConf):

        # This will create the schema defined in the logical data model
        DataLayer.__init__(self,
                           dbID='TRG',
                           dataLayerID='TRG',
                           dataConf=dataConf)

        # We also need to create the "default" components of the target model
        if dataConf.INCLUDE_DM_DATE:
            self.dataModels['TRG'].tables['dm_date'] = \
                TrgTable(self.dataConf,
                         df_dmDate.getSchemaDescription(),
                         self.datastore,
                         dataLayerID='TRG',
                         dataModelID='TRG')

        self.dataModels['TRG'].tables['dm_audit'] = \
            TrgTable(self.dataConf,
                     df_dmAudit.getSchemaDescription(),
                     self.datastore,
                     dataLayerID='TRG',
                     dataModelID='TRG')


class SumDataLayer(DataLayer):

    def __init__(self, dataConf):

        DataLayer.__init__(self,
                           dbID='TRG',
                           dataLayerID='SUM',
                           dataConf=dataConf)
