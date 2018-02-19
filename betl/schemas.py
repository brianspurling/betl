from . import conf
from . import utilities as utils
from . import logger

import psycopg2

import pprint
import csv

import sys

###########
# Globals #
###########

# We don't populate these now, because it makes more sense for main.py to
# run the init funcs first
SRC_LAYER = None
STG_LAYER = None
TRG_LAYER = None
SUM_LAYER = None


###########
# Logging #
###########

log = logger.setUpLogger('SCHEMA', __name__)


###########
# Classes #
###########

#
# A single connection class, that holds connection details for all our
# data source types (e.g. Postgres DB, filesystems, spreadsheets)
#
class Connection():

    def __init__(self, configDetails):

        self.type = configDetails['TYPE']

        if (self.type == 'POSTGRES'):

            self.host = configDetails['HOST']
            self.dbName = configDetails['DBNAME']
            self.userName = configDetails['USERNAME']
            self.password = configDetails['PASSWORD']

            self.connectionString = "host='" + self.host + "' dbname='"       \
                                    + self.dbName + "' user='"                \
                                    + self.userName + "' password='"          \
                                    + self.password + "'"
            self.conn = psycopg2.connect(self.connectionString)

        elif (self.type == 'FILESYSTEM'):
            self.files = {}
            for file in configDetails['FILES']:
                # file is a dictionary like:
                # {filename: , delimiter: , quotechar: }
                self.files[file['filename']] = {'delimiter': file['delimiter'],
                                                'quotechar': file['quotechar']}

        elif (self.type == 'SPREADSHEET'):
            self.worksheets = {}
            msdWorksheets = utils.getMsdWorksheets()
            for worksheet in msdWorksheets:
                # a dictionary indexed by table name, containing the worksheet
                # object
                self.worksheets[worksheet.title] = worksheet

#
# Now starts a set of nested classes that in total define the entire
# for our Postgres ETL and TRG databases. The nested structure is:
#   srcLayer/stgLayer/trgLayer/sumLayer  > dataModel > table > column
#


#
# First up, the column. It holds a set of attributes that describe a single
# column
#
class Column():

    def __init__(self, columnSchema):
        self.columnName = columnSchema['columnName']
        self.dataType = columnSchema['dataType']
        self.isNK = False

        if columnSchema['isNK'].upper() == 'Y':
            self.isNK = True

        # to do #14
        self.columnSchema = columnSchema

    def getSqlCreateStatements(self):
        columnCreateStatement = ''
        if (self.columnSchema['columnType'] == 'Primary key'):
            columnCreateStatement = self.columnName + ' SERIAL UNIQUE'
        else:
            columnCreateStatement = self.columnName + ' ' +                   \
                self.columnSchema['dataType']

        return columnCreateStatement

    def getSqlResetPrimaryKeySequences(self, tableName):
        columnResetStatement = None
        if (self.columnSchema['columnType'] == 'Primary key'):
            seqName = tableName + '_' + self.columnName + '_' + 'key'
            columnResetStatement = 'ALTER SEQUENCE ' + seqName +              \
                'RESTART WITH 1'

        return columnResetStatement

    def __str__(self):
        return '      ' + ', '.join('{} = {}'.format(k, v) for k, v in
                                    self.columnSchema.items()) + '\n'


#
# A table object pretty much just holds a table name and a list of columns
# But for convenience, we hold multiple copies of the column list, with
# different filters applied
#
class Table():

    def __init__(self, tableName, tableSchema):
        self.tableName = tableName.lower()

        # Cut off the SRC_<dataModelId>_ prefix, by doing
        # two "left trims" on the "_" char
        tempTname = tableName[tableName.find("_")+1:]
        tempTname = tempTname[tempTname.find("_")+1:]
        self.tableShortName = tempTname

        self.columns = []

        self.colNameList = []
        self.colNameList = []
        self.nkList = []
        self.nonNkList = []

        for column in tableSchema:

            self.colNameList.append(column['columnName'])

            if column['isNK'].upper() == 'Y':
                self.nkList.append(column['columnName'])
            else:
                self.nonNkList.append(column['columnName'])

            self.colNameList.append(column['columnName'])

            self.columns.append(Column(column))

    def getSqlCreateStatements(self):

        tableCreateStatement = 'CREATE TABLE ' + self.tableName + ' ('

        colsCreateStatements = []
        for columnObject in self.columns:
            colsCreateStatements.append(columnObject.getSqlCreateStatements())

        tableCreateStatement += ', '.join(colsCreateStatements)

        tableCreateStatement += ')'

        return tableCreateStatement

    def getSqlDropStatements(self):

        tableDropStatement = 'DROP TABLE ' + self.tableName

        return tableDropStatement

    def getSqlTruncateStatements(self):

        tableTruncateStatement = 'TRUNCATE ' + self.tableName +               \
            ' RESTART IDENTITY'

        return tableTruncateStatement

    def getSqlResetPrimaryKeySequences(self):

        colsResetStatements = []
        for columnObject in self.columns:
            colResetStatement = columnObject.getSqlResetPrimaryKeySequences(
                tableName=self.tableShortName)
            if colResetStatement is not None:
                colsResetStatements.append(colResetStatement)

        return colsResetStatements

    def __str__(self):
        columnsStr = '\n' + '    ' + self.tableName + '\n'
        for columnObject in self.columns:
            columnsStr += str(columnObject)
        return columnsStr


#
# A data model is a collection of tables, and it sits below dataLayer in the
# hierarchy. In the SRC data layer, a data model is synonymous with a source
# system. In the staging data layer, a data model is synonymous with a "stage"
# in the ETL process. In the target data layer, there is just one data model:
# the target data model. In the summary data layer, there is again just one
# data model: the summary data model.
#
class DataModel():

    def __init__(self, dataModelName, dataModelSchema):
        self.dataModelName = dataModelName
        self.tables = {}
        self.isSchemaDefined = True

        if (len(dataModelSchema) == 0):
            self.isSchemaDefined = False

        for table in dataModelSchema:
            self.tables[table] = Table(table, dataModelSchema[table])

    def getSqlCreateStatements(self):

        tableCreateStatements = []
        for tableName in self.tables:
            tableCreateStatements.append(self.tables[tableName]
                                         .getSqlCreateStatements())
        return tableCreateStatements

    def getSqlDropStatements(self):

        tableDropStatements = []
        for tableName in self.tables:
            tableDropStatements.append(self.tables[tableName]
                                       .getSqlDropStatements())
        return tableDropStatements

    def getSqlTruncateStatements(self):

        tableTruncateStatements = []
        for tableName in self.tables:
            tableTruncateStatements.append(self.tables[tableName]
                                           .getSqlTruncateStatements())
        return tableTruncateStatements

    def getSqlResetPrimaryKeySequences(self):

        tableResetStatements = []
        for tableName in self.tables:
            tableResetStatements.append(self.tables[tableName]
                                        .getSqlResetPrimaryKeySequences())
        return tableResetStatements

    def __str__(self):
        tablesStr = '\n' + '  ** ' + self.dataModelName + ' **' + '\n'
        if (self.isSchemaDefined):
            for tableName in self.tables:
                tablesStr += str(self.tables[tableName])
        else:
            tablesStr += '\n' + '      !! DATA MODEL SCHEMA NOT DEFINED !!'   \
                              + '\n'

        return tablesStr


#
# Finally, our DataLayer objects. We have four data layers: source (SRC),
# staging (STG), target (TRG) and summary (SUM). These are quite radially
# different, so we have a class for each.
#


#
# A SrcLayer() contains 1+ DataModel()s, one for each source system,
# plus one additional for the MSD (manually-sourced data)
# It also contains 1+ srcSystemConns (connection details for each source
# system)
#
# to do #15
class SrcLayer():

    def __init__(self):

        log.debug("START")

        self.srcSystemIds = []
        self.srcSystemSchemaWorksheets = {}
        self.dataModels = {}
        self.srcSystemConns = {}

        # Now populate dataModels with the schema from the SS,
        # and populate srcSystemIds and srcSystemSchemaWorksheets at the same
        # time
        self.loadSchemaFromSpreadsheet()

        # Now, separately, load the connection details for each source system
        # and establish connections to the DBs
        # Even though, in  SRC Data Layer, a source system is synonymous
        # with a DataModel, we hold the connection details separate to the
        # dataModels because dataModel is generic across all DataLayers.
        self.loadSrcSysDBConnections()

    #
    # The ETL DB Schema spreadsheet contains a worksheet per source system,
    # which lists all the columns for all the tables in that source system.
    #
    # So first, we pull these schema out of the SS and load them into our
    # hierarchy of classes
    #
    # Then we pull the MSD schema (manual source data) from the MSD spreadsheet
    # and load that into our hierarchy as one additional Data Model of
    # the SRC Data Layer
    #
    def loadSchemaFromSpreadsheet(self):

        log.debug("START")

        # We'll be building up a dictionary, indexed by dataModelId, containing
        # a dictionary with two elements needed by the DataModel constructor:
        # the worksheet itself, and then a dictionary, indexed by tableName,
        # containing a list of dictionaries, each one indexed by column
        # attribute name, containing the column attribute value

        tmp_dataModels = {}

        # First, get all the relevant worksheets from the ETL DB Schema doc
        srcWorksheets = utils.getSchemaWorksheets('etl', 'src')

        for srcWorksheet in srcWorksheets:

            # Create a new entry in our dataModel dictionary, indexed by the
            # dataModelId (which is the source system ID)

            # Cut off the "ETL.SRC." prefix from the worksheet name
            dmId = srcWorksheet.title[srcWorksheet.title.rfind('.')+1:]
            self.srcSystemIds.append(dmId)
            self.srcSystemSchemaWorksheets[dmId] = srcWorksheet

            tmp_dataModels[dmId] = {'dataModelName': srcWorksheet.title,
                                    'dataModelId':   dmId,
                                    'tableSchemas':  {}}
            tmp_tableSchemas = tmp_dataModels[dmId]['tableSchemas']

            # Load the schema for this DataModel out of the spreadsheet
            dataModelSchema_allRows = srcWorksheet.get_all_records()

            # We're working through a list of cols spanning multiple tables, so
            # detect when we move to a new table
            for schemaRow in dataModelSchema_allRows:
                if schemaRow['Table Name'].lower() not in tmp_tableSchemas:
                    # Insert a new table into our dictionary,
                    # with an empty list of columns
                    tmp_tableSchemas[schemaRow['Table Name'].lower()] = []

                # And add the current column to this table's list of columns.
                # Each list item is a dictionary of column metadata
                colums = tmp_tableSchemas[schemaRow['Table Name'].lower()]
                colums.append({'columnName': schemaRow['Column Name'],
                               'dataType':   schemaRow['data_type'],
                               'isNK':       schemaRow['natural_key']})

        # Next, the MSD: We're going to add one more dataModel, which will
        # include all manual source data from the MSD spreadsheet

        msdWorksheets = utils.getMsdWorksheets()
        dmId = 'MSD'
        tmp_dataModels[dmId] = {'dataModelName': 'MSD',
                                'dataModelId':   dmId,
                                'tableSchemas': {}}
        tmp_tableSchemas = tmp_dataModels[dmId]['tableSchemas']

        for msdWorksheet in msdWorksheets:

            # Insert a new table into our dictionary, with an empty list of
            # columns.
            tmp_tableSchemas[msdWorksheet.title] = []

            # The first row of the spreadsheet has column names
            # The second row has data types
            # The third row has isNKs
            columnNames = msdWorksheet.row_values(1)
            dataTypes = msdWorksheet.row_values(2)
            isNKs = msdWorksheet.row_values(3)

            for i in range(len(columnNames)):
                if columnNames[i] == '':
                    break  # to stop reading empty cells as values

                # Add the current column to this table's list of columns.
                # Each list item is a dictionary of column metadata indexed by
                # column name
                colums = tmp_tableSchemas[msdWorksheet.title]
                colums.append({'columnName': columnNames[i],
                               'dataType':   dataTypes[i],
                               'isNK':       isNKs[i]})

        # We have all the schema data now, so create the DataModel() object.
        # This is the object we "leave behind" in this class - the full schema
        # gets passed down to "child" constructors
        for i in tmp_dataModels:
            self.dataModels[tmp_dataModels[i]['dataModelId']] =               \
                    DataModel(tmp_dataModels[i]['dataModelName'],
                              tmp_dataModels[i]['tableSchemas'])

            log.info("Loaded schema for data model: " +
                     tmp_dataModels[i]['dataModelName'])

    def loadSrcSysDBConnections(self):

        log.debug("START")

        for srcSysId in self.srcSystemIds:
            # Get the srcSystemConn details from settings and dump them into
            # our dictionary
            self.srcSystemConns[srcSysId] = Connection(conf.SOURCE_SYSTEM_CONNS
                                                       [srcSysId])

        # to do #16
        self.srcSystemConns['MSD'] = Connection(conf.SOURCE_SYSTEM_CONNS
                                                ['MSD'])
        log.info("Loaded connections to " + str(len(self.srcSystemIds)) +
                 " source systems")

    def dropPhysicalDataModel(self):

        log.debug("START")

        dropStatements = self.getSqlDropStatements()

        etlDbCursor = conf.ETL_DB_CONN.cursor()
        counter = 0
        for dropStatement in dropStatements:
            try:
                etlDbCursor.execute(dropStatement)
                conf.ETL_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                # to do #17
                conf.ETL_DB_CONN.commit()
                pass

        log.info("Dropped " + str(counter) + ' tables')

    def autoPopulateSrcLayerSchemasInSpreadsheet(self, dataModelId):

        log.debug("START")

        srcSysType = self.srcSystemConns[dataModelId].type

        # Schema is a list of tuples:
        # (data modelid, data model type,
        #  table/file name, column name, column details...)
        schema = []

        if self.srcSystemConns[dataModelId].type == 'POSTGRES':

            log.info("Source system type is POSTGRES: connecting and " +
                     "pulling list of tables and columns")

            # Get the schema from Postgres
            srcDbCursor = self.srcSystemConns[dataModelId].connection.cursor()
            srcDbCursor.execute("SELECT * FROM information_schema.columns c " +
                                "WHERE c.table_schema = 'public'")
            srcColumns = srcDbCursor.fetchall()

            for row in srcColumns:
                currentTableName = row[2]

                newCol = [dataModelId,
                          self.srcSystemConns[dataModelId].type,
                          'src_' + dataModelId + '_' + currentTableName,
                          row[3], row[6], row[7], row[8]]
                schema.append(newCol)

        elif self.srcSystemConns[dataModelId].type == 'FILESYSTEM':

            log.info("Source system type is FILESYSTEM (aka CSV): " +
                     "connecting  and pulling list of columns")

            # Get the schema from the files
            # to do #18
            csv.field_size_limit(1131072)
            for filename in self.srcSystemConns[dataModelId].files:
                with open(filename + '.csv', "r") as srcFile:
                    delimiter = self.srcSystemConns[dataModelId]              \
                        .files[filename]['delimiter']
                    quoteChar = self.srcSystemConns[dataModelId].             \
                        files[filename]['quotechar']
                    csvReader = csv.reader(srcFile,
                                           delimiter=delimiter,
                                           quotechar=quoteChar)
                    csvColumns = next(csvReader)

                for csvColumn in csvColumns:
                    # to do #19
                    schema.append([dataModelId,
                                   srcSysType,
                                   'src_' + dataModelId + '_'
                                   + filename,
                                   csvColumn,
                                   'YES',
                                   'character varying',
                                   128])

        else:
            raise ValueError("Failed to rebuild SRC Layer: Source system " +
                             "type is " +
                             self.srcSystemConns[dataModelId].type +
                             ". Stopping execution. We only " +
                             "deal with 'POSTGRES' and 'FILESYSTEM' source " +
                             "system types, so cannot auto-populate the " +
                             "ETL.SRC schemas for this source system")

        # Check we managed to find some kind of schema
        # from the source system, otherwise abort
        if (len(schema) == 0):
            raise ValueError("Failed to rebuild SRC Layer: SRC data layer " +
                             "in the ETL DB schema spreadsheet is not yet " +
                             "populated, so we tried to pull the schema " +
                             "from the source systems directly, but we " +
                             "could not find anything")

        # We build up our new GSheets table first, in memory,
        # then write it all in one go.

        schemaWS = self.srcSystemSchemaWorksheets[dataModelId]
        cell_list = schemaWS.range('A2:G'+str(len(schema)+1))
        rangeRowCount = 0
        for schemaRow in schema:
            cell_list[rangeRowCount].value = schemaRow[0]
            cell_list[rangeRowCount+1].value = schemaRow[1]
            cell_list[rangeRowCount+2].value = schemaRow[2]
            cell_list[rangeRowCount+3].value = schemaRow[3]
            cell_list[rangeRowCount+4].value = schemaRow[4]
            cell_list[rangeRowCount+5].value = schemaRow[5]
            cell_list[rangeRowCount+6].value = schemaRow[6]
            rangeRowCount += 7

        self.dataModels[dataModelId].schemaWS.update_cells(cell_list)

        log.info("SRC schema updated in worksheet: " + schemaWS.title)

    def rebuildPhsyicalDataModel(self):

        log.info("START (src)")

        # First, we need to drop every source table (prefixed SRC_)  - we
        # are clearing out and starting again
        log.info("Dropping all SRC tables")
        self.dropPhysicalDataModel()

        haveWeChangedSchemaSS = False

        for dataModelId in self.dataModels:

            # If this worksheet of the Schema doc is not already populated, we
            # populate it with an exact copy of the source DB's schema.
            # Obviously don't do this for the MSD

            if dataModelId == 'MSD':
                break

            if (self.dataModels[dataModelId].isSchemaDefined):
                log.info("the schema worksheet for source system <" +
                         dataModelId + "> " + "is already populated")
            else:
                log.info("the schema worksheet for source system <" +
                         dataModelId + "> " + "is NOT yet populated")
                haveWeChangedSchemaSS = True
                self.autoPopulateSrcLayerSchemasInSpreadsheet(dataModelId)

        if haveWeChangedSchemaSS:
            # Finally, having just updated the spreadsheet, we do a complete
            # reload of the Data Layers, so our Data Layer objects are all
            # up to date
            log.info("Reloading the schema from the spreadsheet")
            self.loadSchemaFromSpreadsheet()

        #
        # The schema worksheet is now populated, either with a direct copy of
        # the source schema, or with a bespoke configuration entered/edited
        # directly in the spreadsheet. We now need to clear out any existing
        # SRC staging tables and recreate
        #

        log.info("Recreating all SRC tables")
        createStatements = self.getSqlCreateStatements()

        # Then create the tables

        etlDbCursor = conf.ETL_DB_CONN.cursor()
        counter = 0
        for createStatement in createStatements:
            try:
                etlDbCursor.execute(createStatement)
                conf.ETL_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                pass

        log.info("Created " + str(counter) + ' tables')

    def getSqlCreateStatements(self):
        log.debug("START")
        dataLayerCreateStatements = []
        for dataModelId in self.dataModels:
            dataLayerCreateStatements.extend(
                self.dataModels[dataModelId].getSqlCreateStatements())

        return dataLayerCreateStatements

    def getSqlDropStatements(self):
        log.debug("START")
        dataLayerDropStatements = []
        for dataModelId in self.dataModels:
            dataLayerDropStatements.extend(
                self.dataModels[dataModelId].getSqlDropStatements())

        return dataLayerDropStatements

    def __str__(self):
        dataModelStr = '\n' + '\n' + '*** Data Layer: Source ***' + '\n'
        for dataModelId in self.dataModels:
            dataModelStr += str(self.dataModels[dataModelId])

        return dataModelStr


#
# A StgLayer() contains 0+ DataModel()s, one for each peristent stage
# in the ETL's transformation process
#
class StgLayer():

    def __init__(self):

        log.debug("START")

        self.dataModels = {}

        # Now populate dataModels with the schema from the SS
        self.loadSchemaFromSpreadsheet()

    #
    # The ETL DB Schema spreadsheet contains a worksheet per table for all
    # data models in the staging data layer. Each worksheet lists the columns
    # in the table
    #
    # So we pull these schema out of the SS and load them into our
    # hierarchy of classes
    #
    def loadSchemaFromSpreadsheet(self):

        log.debug("START")

        # We'll be building up a dictionary, indexed by dataModelId, containing
        # a dictionary with two elements needed by the DataModel constructor:
        # the worksheet itself, and then a dictionary, indexed by tableName,
        # containing a list of dictionaries, each one indexed by column
        # attribute name, containing the column attribute value

        tmp_dataModels = {}
        tmp_tableSchemas = {}

        # First, get all the relevant worksheets from the ETL DB Schema doc
        stgWorksheets = utils.getSchemaWorksheets('etl', 'stg')

        for stgWorksheet in stgWorksheets:

            # Cut off the table name "suffix" from the worksheet name
            # and the "ETL.STG." prefix to get the dataModel Id
            # Cut off the ETL.STG.---. prefix to et the table name
            dmId = stgWorksheet.title[:stgWorksheet.title.rfind('.')]
            dmId = dmId[dmId.rfind('.')+1:]
            tableName = stgWorksheet.title[stgWorksheet.title.rfind('.')+1:]
            tableName = tableName.lower()

            # We're working through a list of worksheets spanning multiple
            # dataModels, so detect when we move to a new data model
            if dmId not in tmp_dataModels:

                # Create a new entry in our dataModel dictionary, indexed by
                # the dataModelId
                tmp_dataModels[dmId] = {'dataModelName': dmId,
                                        'dataModelId':   dmId,
                                        'tableSchemas':  {}}
                tmp_tableSchemas = tmp_dataModels[dmId]['tableSchemas']

            # Add this current worksheet's table to our dict of tables, with
            # an empty list of columns
            tmp_tableSchemas[tableName] = []

            # Load the schema for this table out of the spreadsheet
            tableSchema_allRows = stgWorksheet.get_all_records()

            # And add the columns to this table's list of columns.
            for columnRow in tableSchema_allRows:
                # Each list item is a dictionary of column metadata
                columns = tmp_tableSchemas[tableName]
                columns.append({'columnName': columnRow['Column Name'],
                                'dataType':   columnRow['Data Type'],
                                'columnType': columnRow['Column Type'],
                                'isNK':       'N'})

        # We have all the schema data now, so create the DataModel() object.
        # This is the object we "leave behind" in this class - the full schema
        # gets passed down to "child" constructors
        for i in tmp_dataModels:
            self.dataModels[tmp_dataModels[i]['dataModelId']] =               \
                    DataModel(tmp_dataModels[i]['dataModelName'],
                              tmp_dataModels[i]['tableSchemas'])

            log.info("Loaded schema for data model: " +
                     tmp_dataModels[i]['dataModelName'])

    def dropPhysicalDataModel(self):

        log.debug("START")

        dropStatements = self.getSqlDropStatements()

        etlDbCursor = conf.ETL_DB_CONN.cursor()
        counter = 0
        for dropStatement in dropStatements:
            try:
                etlDbCursor.execute(dropStatement)
                conf.ETL_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                # to do #20
                conf.ETL_DB_CONN.commit()

                pass

        log.info("Dropped " + str(counter) + ' tables')

    def rebuildPhsyicalDataModel(self):

        log.info("START (stg)")

        # First, we need to drop every staging table  - we
        # are clearing out and starting again
        log.info("Dropping all STG tables")
        self.dropPhysicalDataModel()

        # Then create the tables

        log.info("Recreating all STG tables")
        createStatements = self.getSqlCreateStatements()

        etlDbCursor = conf.ETL_DB_CONN.cursor()
        counter = 0
        for createStatement in createStatements:
            try:
                etlDbCursor.execute(createStatement)
                conf.ETL_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                pass

        log.info("Created " + str(counter) + ' tables')

    def getSqlCreateStatements(self):
        log.debug("START")
        dataLayerCreateStatements = []
        for dataModelId in self.dataModels:
            dataLayerCreateStatements.extend(
                self.dataModels[dataModelId].getSqlCreateStatements())

        return dataLayerCreateStatements

    def getSqlDropStatements(self):
        log.debug("START")
        dataLayerDropStatements = []
        for dataModelId in self.dataModels:
            dataLayerDropStatements.extend(
                self.dataModels[dataModelId].getSqlDropStatements())

        return dataLayerDropStatements

    def __str__(self):
        dataModelStr = '\n' + '\n' + '*** Data Layer: Staging ***' + '\n'
        for dataModelId in self.dataModels:
            dataModelStr += str(self.dataModels[dataModelId])

        return dataModelStr


#
# A TrgLayer() contains # 1 Data Model, the Target data model
#
class TrgLayer():

    def __init__(self):

        log.debug("START")

        self.dataModels = {}

        # Now populate dataModels with the schema from the SS
        self.loadSchemaFromSpreadsheet()

    #
    # The TRG DB Schema spreadsheet contains a worksheet per table for all
    # data models in the target and summary data layers. Each worksheet
    # lists the columns in the table
    #
    # So we pull these schema out of the SS and load them into our
    # hierarchy of classes
    #
    def loadSchemaFromSpreadsheet(self):

        log.debug("START")

        # We'll be building up a dictionary, indexed by dataModelId, containing
        # a dictionary with two elements needed by the DataModel constructor:
        # the worksheet itself, and then a dictionary, indexed by tableName,
        # containing a list of dictionaries, each one indexed by column
        # attribute name, containing the column attribute value

        tmp_dataModels = {}
        tmp_dataModels['TRG'] = {'dataModelName': 'TRG',
                                 'dataModelId':   'TRG',
                                 'tableSchemas':  {}}
        tmp_tableSchemas = {}

        # First, get all the relevant worksheets from the TRG DB Schema doc
        trgWorksheets = utils.getSchemaWorksheets('trg', 'trg')

        for trgWorksheet in trgWorksheets:

            # Cut off the TRG.TRG.TRG. prefix to get the table name
            tableName = trgWorksheet.title[trgWorksheet.title.rfind('.')+1:]
            tableName = tableName.lower()

            # Create a new entry in our dataModel dictionary, indexed by
            # the dataModelId

            tmp_tableSchemas = tmp_dataModels['TRG']['tableSchemas']

            # Add this current worksheet's table to our dict of tables, with
            # an empty list of columns
            tmp_tableSchemas[tableName] = []

            # Load the schema for this table out of the spreadsheet
            tableSchema_allRows = trgWorksheet.get_all_records()

            # And add the columns to this table's list of columns.
            for columnRow in tableSchema_allRows:
                # Each list item is a dictionary of column metadata
                columns = tmp_tableSchemas[tableName]
                columns.append({'columnName': columnRow['Column Name'],
                                'dataType':   columnRow['Data Type'],
                                'columnType': columnRow['Column Type'],
                                'isNK':       'N'})

        # We have all the schema data now, so create the DataModel() object.
        # This is the object we "leave behind" in this class - the full schema
        # gets passed down to "child" constructors
        for i in tmp_dataModels:
            self.dataModels[tmp_dataModels[i]['dataModelId']] =               \
                    DataModel(tmp_dataModels[i]['dataModelName'],
                              tmp_dataModels[i]['tableSchemas'])

            log.info("Loaded schema for data model: " +
                     tmp_dataModels[i]['dataModelName'])

    def dropPhysicalDataModel(self):

        log.debug("START")

        dropStatements = self.getSqlDropStatements()

        trgDbCursor = conf.TRG_DB_CONN.cursor()
        counter = 0
        for dropStatement in dropStatements:
            try:
                trgDbCursor.execute(dropStatement)
                conf.TRG_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                # to do #20
                conf.TRG_DB_CONN.commit()
                pass

        log.info("Dropped " + str(counter) + ' tables')

    def truncatePhysicalDataModel(self):

        log.debug("START")

        truncateStatements = self.getSqlTruncateStatements()

        trgDbCursor = conf.TRG_DB_CONN.cursor()
        counter = 0
        for truncateStatement in truncateStatements:
            try:
                trgDbCursor.execute(truncateStatement)
                conf.TRG_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                # to do #20
                conf.TRG_DB_CONN.commit()
                pass

        log.info("Dropped " + str(counter) + ' tables')

    def rebuildPhsyicalDataModel(self):

        log.info("START (trg)")

        # First, we need to drop every staging table  - we
        # are clearing out and starting again
        log.info("Dropping all TRG tables")
        self.dropPhysicalDataModel()

        # Then create the tables

        log.info("Recreating all TRG tables")
        createStatements = self.getSqlCreateStatements()

        trgDbCursor = conf.TRG_DB_CONN.cursor()
        counter = 0
        for createStatement in createStatements:
            try:
                trgDbCursor.execute(createStatement)
                conf.TRG_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                pass

        log.info("Created " + str(counter) + ' tables')

    def resetPrimaryKeySequences(self):

        log.info("START (trg)")

        resetStatements = self.getSqlResetPrimaryKeySequences()

        sys.exit()
        trgDbCursor = conf.TRG_DB_CONN.cursor()
        counter = 0
        for resetStatement in resetStatements:
            try:
                trgDbCursor.execute(resetStatement)
                conf.TRG_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                pass

        log.info("Reset " + str(counter) + ' primary key sequences')

    def getSqlCreateStatements(self):
        log.debug("START")
        dataLayerCreateStatements = []
        for dataModelId in self.dataModels:
            dataLayerCreateStatements.extend(
                self.dataModels[dataModelId].getSqlCreateStatements())

        return dataLayerCreateStatements

    def getSqlDropStatements(self):
        log.debug("START")
        dataLayerDropStatements = []
        for dataModelId in self.dataModels:
            dataLayerDropStatements.extend(
                self.dataModels[dataModelId].getSqlDropStatements())

        return dataLayerDropStatements

    def getSqlTruncateStatements(self):
        log.debug("START")
        dataLayerTruncateStatements = []
        for dataModelId in self.dataModels:
            dataLayerTruncateStatements.extend(
                self.dataModels[dataModelId].getSqlTruncateStatements())

        return dataLayerTruncateStatements

    def getSqlResetPrimaryKeySequences(self):
        log.debug("START")
        dataLayerResetStatements = []
        for dataModelId in self.dataModels:
            dataLayerResetStatements.extend(
                self.dataModels[dataModelId].getSqlResetPrimaryKeySequences())

        return dataLayerResetStatements

    def __str__(self):
        dataModelStr = '\n' + '\n' + '*** Data Layer: Staging ***' + '\n'
        for dataModelId in self.dataModels:
            dataModelStr += str(self.dataModels[dataModelId])

        return dataModelStr


#
# A SumLayer() contains 1 Data Model, the Summary data model
#
class SumLayer():

    def __init__(self):

        log.debug("START")

        self.dataModels = {}

        # Now populate dataModels with the schema from the SS
        self.loadSchemaFromSpreadsheet()

    #
    # The TRG DB Schema spreadsheet contains a worksheet per table for all
    # data models in the target and summary data layers. Each worksheet
    # lists the columns in the table
    #
    # So we pull these schema out of the SS and load them into our
    # hierarchy of classes
    #
    def loadSchemaFromSpreadsheet(self):

        log.debug("START")

        # We'll be building up a dictionary, indexed by dataModelId, containing
        # a dictionary with two elements needed by the DataModel constructor:
        # the worksheet itself, and then a dictionary, indexed by tableName,
        # containing a list of dictionaries, each one indexed by column
        # attribute name, containing the column attribute value

        tmp_dataModels = {}
        tmp_dataModels['SUM'] = {'dataModelName': 'SUM',
                                 'dataModelId':   'SUM',
                                 'tableSchemas':  {}}
        tmp_tableSchemas = {}

        # First, get all the relevant worksheets from the TRG DB Schema doc
        sumWorksheets = utils.getSchemaWorksheets('trg', 'sum')

        for sumWorksheet in sumWorksheets:

            # Cut off the TRG.SUM.SUM. prefix to get the table name
            tableName = sumWorksheet.title[sumWorksheet.title.rfind('.')+1:]
            tableName = tableName.lower()

            # Create a new entry in our dataModel dictionary, indexed by
            # the dataModelId

            tmp_tableSchemas = tmp_dataModels['SUM']['tableSchemas']

            # Add this current worksheet's table to our dict of tables, with
            # an empty list of columns
            tmp_tableSchemas[tableName] = []

            # Load the schema for this table out of the spreadsheet
            tableSchema_allRows = sumWorksheet.get_all_records()

            # And add the columns to this table's list of columns.
            for columnRow in tableSchema_allRows:
                # Each list item is a dictionary of column metadata
                columns = tmp_tableSchemas[tableName]
                columns.append({'columnName': columnRow['Column Name'],
                                'dataType':   columnRow['Data Type'],
                                'columnType': columnRow['Column Type'],
                                'isNK':       'N'})

        # We have all the schema data now, so create the DataModel() object.
        # This is the object we "leave behind" in this class - the full schema
        # gets passed down to "child" constructors
        for i in tmp_dataModels:
            self.dataModels[tmp_dataModels[i]['dataModelId']] =               \
                    DataModel(tmp_dataModels[i]['dataModelName'],
                              tmp_dataModels[i]['tableSchemas'])

            log.info("Loaded schema for data model: " +
                     tmp_dataModels[i]['dataModelName'])

    def dropPhysicalDataModel(self):

        log.debug("START")

        dropStatements = self.getSqlDropStatements()

        trgDbCursor = conf.TRG_DB_CONN.cursor()
        counter = 0
        for dropStatement in dropStatements:
            try:
                trgDbCursor.execute(dropStatement)
                conf.TRG_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                # to do #20
                conf.TRG_DB_CONN.commit()
                pass

        log.info("Dropped " + str(counter) + ' tables')

    def rebuildPhsyicalDataModel(self):

        log.info("START (SUM)")

        # First, we need to drop every staging table  - we
        # are clearing out and starting again
        log.info("Dropping all SUM tables")
        self.dropPhysicalDataModel()

        # Then create the tables

        log.info("Recreating all SUM tables")
        createStatements = self.getSqlCreateStatements()

        trgDbCursor = conf.TRG_DB_CONN.cursor()
        counter = 0
        for createStatement in createStatements:
            try:
                trgDbCursor.execute(createStatement)
                conf.TRG_DB_CONN.commit()
                counter += 1

            except psycopg2.Error as e:
                pprint.pprint(e)
                pass

        log.info("Created " + str(counter) + ' tables')

    def getSqlCreateStatements(self):
        log.debug("START")
        dataLayerCreateStatements = []
        for dataModelId in self.dataModels:
            dataLayerCreateStatements.extend(
                self.dataModels[dataModelId].getSqlCreateStatements())

        return dataLayerCreateStatements

    def getSqlDropStatements(self):
        log.debug("START")
        dataLayerDropStatements = []
        for dataModelId in self.dataModels:
            dataLayerDropStatements.extend(
                self.dataModels[dataModelId].getSqlDropStatements())

        return dataLayerDropStatements

    def __str__(self):
        dataModelStr = '\n' + '\n' + '*** Data Layer: Summary ***' + '\n'
        for dataModelId in self.dataModels:
            dataModelStr += str(self.dataModels[dataModelId])

        return dataModelStr


def getSrcLayerSchema():
    global SRC_LAYER
    return SRC_LAYER
