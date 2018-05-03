from . import logger
from .dataModel import DataModel
from .dataModel import SrcDataModel
from .table import TrgTable
from . import df_dmDate
from . import df_dmAudit

import ast


class DataLayer():

    def __init__(self, dbID, dataLayerID, conf):

        self.conf = conf
        self.databaseID = dbID
        self.dataLayerID = dataLayerID

        self.datastore = conf.data.getDatastore(dbID)
        self.dataModels = self.buildLogicalDataModels()

    def buildLogicalDataModels(self):

        file = open('schemas/dbSchemaDesc_' + self.databaseID + '.txt', 'r')
        dbSchemaDesc = ast.literal_eval(file.read())
        dlSchemaDesc = dbSchemaDesc[self.dataLayerID]
        dataModels = {}

        for dataModelID in dlSchemaDesc['dataModelSchemas']:
            if self.dataLayerID == 'SRC':
                # Each dataModel in the SRC dataLayer is a source system
                dataModels[dataModelID] = \
                    SrcDataModel(self.conf,
                                 dlSchemaDesc['dataModelSchemas'][dataModelID],
                                 self.datastore,
                                 self.dataLayerID)
            else:
                dataModels[dataModelID] = \
                    DataModel(self.conf,
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

        # This will create the schema defined in the logical data model
        DataLayer.__init__(self,
                           dbID='TRG',
                           dataLayerID='TRG',
                           conf=conf)

        # We also need to create the "default" components of the target model
        if conf.schedule.DEFAULT_DM_DATE:
            self.dataModels['TRG'].tables['dm_date'] = \
                TrgTable(self.conf,
                         df_dmDate.getSchemaDescription(),
                         self.datastore,
                         dataLayerID='TRG',
                         dataModelID='TRG')

        self.dataModels['TRG'].tables['dm_audit'] = \
            TrgTable(self.conf,
                     df_dmAudit.getSchemaDescription(),
                     self.datastore,
                     dataLayerID='TRG',
                     dataModelID='TRG')


class SumDataLayer(DataLayer):

    def __init__(self, conf):

        DataLayer.__init__(self,
                           dbID='TRG',
                           dataLayerID='SUM',
                           conf=conf)
