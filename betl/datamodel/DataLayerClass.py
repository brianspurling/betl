from betl.logger import logger
from betl.logger import alerts
from .DatasetClass import Dataset
from .DatasetClass import SrcDataset
from .DatasetClass import EmptyDataset
from .TableClass import TrgTable
from betl.defaultdataflows import dmDate
from betl.defaultdataflows import dmAudit

import ast


class DataLayer():

    def __init__(self, conf, dbID, dataLayerID, dataModels):

        self.conf = conf
        self.databaseID = dbID
        self.dataLayerID = dataLayerID
        self.dataModels = dataModels
        self.datastore = conf.DATA.getDWHDatastore(dbID)

    def buildPhysicalSchema(self):

        self.dropPhysicalSchema()

        createStatements = self.getSqlCreateStatements()

        dbCursor = self.datastore.cursor()
        for createStatement in createStatements:
            dbCursor.execute(createStatement)
            self.datastore.commit()

        logger.logRebuildingPhysicalDataModel(self.dataLayerID)

    def dropPhysicalSchema(self):

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
        if self.dataModels is not None:
            for dataModelID in self.dataModels:
                c = self.dataModels[dataModelID].getColumnsForTable(tableName)
                if c is not None:
                    return c
        else:
            # It's possible for there to be no schema desc for a data layer
            return None

    def __str__(self):
        string = ('\n' + '*** Data Layer: ' +
                  self.dataLayerID + ' ***' + '\n')
        for dataModelID in self.dataModels:
            string += str(self.dataModels[dataModelID])
        return string


class SrcDataLayer(DataLayer):

    def __init__(self, conf):

        self.conf = conf
        self.databaseID = 'ETL'
        self.dataLayerID = 'SRC'

        self.datastore = conf.DATA.getDWHDatastore('ETL')

        schemaFile = open('schemas/dbSchemaDesc_ETL.txt', 'r')
        self.dbSchemaDesc = ast.literal_eval(schemaFile.read())

        self.dataModels = {}

        if 'SRC' not in self.dbSchemaDesc:

            alert = 'Did not find a schema description for datalayer SRC'
            alerts.logAlert(self.conf, alert)
            self.dataModels['SRC'] = EmptyDataset()

        else:

            dlSchemaDesc = self.dbSchemaDesc['SRC']

            try:
                mapFile = open('schemas/tableNameMapping.txt', 'r')
                tableNameMap = ast.literal_eval(mapFile.read())
            except FileNotFoundError:
                tableNameMap = None
                mappedTableName = None

            for dmId in dlSchemaDesc['dataModelSchemas']:

                if tableNameMap is not None:
                    mappedTableName = tableNameMap[dmId]

                # Each dataModel in the SRC dataLayer is a source system
                self.dataModels[dmId] = SrcDataset(
                        dataConf=conf.DATA,
                        dmSchemaDesc=dlSchemaDesc['dataModelSchemas'][dmId],
                        tableNameMap=mappedTableName,
                        datastore=self.datastore,
                        dataLayerID=self.dataLayerID)

        DataLayer.__init__(self,
                           dbID='ETL',
                           dataLayerID='SRC',
                           conf=conf,
                           dataModels=self.dataModels)


class StgDataLayer(DataLayer):

    def __init__(self, conf):

        self.conf = conf
        self.databaseID = 'ETL'
        self.dataLayerID = 'STG'

        self.datastore = conf.DATA.getDWHDatastore('ETL')

        schemaFile = open(conf.CTRL.SCHEMA_PATH + '/dbSchemaDesc_ETL.txt', 'r')
        self.dbSchemaDesc = ast.literal_eval(schemaFile.read())

        self.dataModels = {}

        if 'STG' not in self.dbSchemaDesc:

            alert = 'Did not find a schema description for datalayer STG'
            alerts.logAlert(self.conf, alert)
            self.dataModels['STG'] = EmptyDataset()

        else:

            dlSchemaDesc = self.dbSchemaDesc['STG']

            for dmId in dlSchemaDesc['dataModelSchemas']:

                # Each dataModel in the STG dataLayer is an optionally-defined
                # schema through which the ETL transformations move the data

                self.dataModels[dmId] = Dataset(
                    dataConf=self.conf.DATA,
                    dmSchemaDesc=dlSchemaDesc['dataModelSchemas'][dmId],
                    datastore=self.datastore,
                    dataLayerID=self.dataLayerID)

        DataLayer.__init__(self,
                           dbID='ETL',
                           dataLayerID='STG',
                           conf=conf,
                           dataModels=self.dataModels)


class TrgDataLayer(DataLayer):

    def __init__(self, conf):

        self.conf = conf
        self.databaseID = 'TRG'
        self.dataLayerID = 'TRG'

        self.datastore = conf.DATA.getDWHDatastore('TRG')

        schemaFile = open('schemas/dbSchemaDesc_TRG.txt', 'r')
        self.dbSchemaDesc = ast.literal_eval(schemaFile.read())

        self.dataModels = {}

        if 'TRG' not in self.dbSchemaDesc:

            alert = 'Did not find a schema description for datalayer TRG'
            alerts.logAlert(self.conf, alert)

            # Unlike the other datalayers, we don't create an empty Dataset
            # because we must have, at least, a TRG datamodel

            self.dataModels['TRG'] = Dataset(
                dataConf=self.conf.DATA,
                dmSchemaDesc=None,
                datastore=self.datastore,
                dataLayerID=self.dataLayerID)

        else:

            dlSchemaDesc = self.dbSchemaDesc['TRG']

            for dmId in dlSchemaDesc['dataModelSchemas']:

                # There's usually only one dataModels in the TRG dataLayer: TRG
                # TODO: confirm, can you have custom TRG dataModels?

                self.dataModels[dmId] = Dataset(
                    dataConf=self.conf.DATA,
                    dmSchemaDesc=dlSchemaDesc['dataModelSchemas'][dmId],
                    datastore=self.datastore,
                    dataLayerID=self.dataLayerID)

        # We also need to create the "default" components of the target model

        if conf.SCHEDULE.DEFAULT_DM_DATE:
            self.dataModels['TRG'].tables['dm_date'] = \
                TrgTable(self.conf.DATA,
                         dmDate.getSchemaDescription(),
                         self.datastore,
                         dataLayerID='TRG',
                         dataModelID='TRG')

        if conf.SCHEDULE.DEFAULT_DM_AUDIT:
            self.dataModels['TRG'].tables['dm_audit'] = \
                TrgTable(self.conf.DATA,
                         dmAudit.getSchemaDescription(),
                         self.datastore,
                         dataLayerID='TRG',
                         dataModelID='TRG')

        DataLayer.__init__(self,
                           dbID='TRG',
                           dataLayerID='TRG',
                           conf=conf,
                           dataModels=self.dataModels)


class SumDataLayer(DataLayer):

    def __init__(self, conf):

        self.conf = conf
        self.databaseID = 'TRG'
        self.dataLayerID = 'SUM'

        self.datastore = conf.DATA.getDWHDatastore('TRG')

        schemaFile = open('schemas/dbSchemaDesc_TRG.txt', 'r')
        self.dbSchemaDesc = ast.literal_eval(schemaFile.read())

        self.dataModels = {}

        if 'SUM' not in self.dbSchemaDesc:

            alert = 'Did not find a schema description for datalayer SUM'
            alerts.logAlert(self.conf, alert)
            self.dataModels['SUM'] = EmptyDataset()

        else:

            dlSchemaDesc = self.dbSchemaDesc['SUM']

            for dmId in dlSchemaDesc['dataModelSchemas']:

                # There's usually only one dataModels in the SUM dataLayer: SUM
                # TODO: confirm, can you have custom SUM dataModels?

                self.dataModels[dmId] = Dataset(
                    dataConf=self.conf.DATA,
                    dmSchemaDesc=dlSchemaDesc['dataModelSchemas'][dmId],
                    datastore=self.datastore,
                    dataLayerID=self.dataLayerID)

        DataLayer.__init__(self,
                           dbID='TRG',
                           dataLayerID='SUM',
                           conf=conf,
                           dataModels=self.dataModels)
