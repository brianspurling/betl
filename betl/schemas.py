from . import conf
from . import utilities as utils

import psycopg2

import pprint
import csv

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
log = utils.setUpLogger('SCHEMA', __name__)


###########
# Classes #
###########

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


class Column():

    # columnSchema must be a schema list like this:
    # {columnName:<columnName>,dataType:<dataType>}
    def __init__(self, columnSchema):
        self.columnName = columnSchema['columnName']
        self.dataType = columnSchema['dataType']
        self.isNK = False
        self.isAudit = False

        if columnSchema['isNK'].upper() == 'Y':
            self.isNK = True
        if columnSchema['isAudit'].upper() == 'Y':
            self.isAudit = True

        self.columnSchema = columnSchema

    def getSqlCreateStatements(self):
        return self.columnName + ' ' + self.columnSchema['dataType']

    def __str__(self):
        return '      ' + ', '.join('{} = {}'.format(k, v) for k, v in
                                    self.columnSchema.items()) + '\n'


class Table():

    # To do: we will lose the NKs if we rebuild the schema from source.
    # Consider making the ETL.SRC.<dataModelId> sheets the dumping ground,
    # with the STM proper looking up from them and holding the NK somehow

    # tableSchema must be a schema list like this: [{column metadata}],
    def __init__(self, tableName, tableSchema):
        self.tableName = tableName.lower()

        # Cut off the SRC_<dataModelId>_ prefix, by doing
        # two "left trims" on the "_" char
        tempTname = tableName[tableName.find("_")+1:]
        tempTname = tempTname[tempTname.find("_")+1:]
        self.tableShortName = tempTname

        self.columns = []

        self.columnList = []
        self.columnList_withoutAudit = []
        self.nkList = []
        self.nonNkList = []

        for column in tableSchema:

            self.columnList.append(column['columnName'])

            if column['isNK'].upper() == 'Y':
                self.nkList.append(column['columnName'])
            else:
                self.nonNkList.append(column['columnName'])

            if column['isAudit'].upper() != 'Y':
                self.columnList_withoutAudit.append(column['columnName'])

            self.columns.append(Column(column))

    def getSqlCreateStatements(self):

        tableCreateStatement = 'CREATE TABLE ' + self.tableName + ' ('

        columns = []
        for columnSchema in self.columns:
            columns.append(columnSchema.getSqlCreateStatements())
        tableCreateStatement += ', '.join(columns)

        tableCreateStatement += ')'

        return tableCreateStatement

    def __str__(self):
        columnsStr = '\n' + '    ' + self.tableName + '\n'
        for columnSchema in self.columns:
            columnsStr += str(columnSchema)
        return columnsStr


class DataModel():

    # dataModelSchema must be a schema dictionary like this:
    # Tables > [{column metadata}],
    def __init__(self, dataModelName, dataModelSchema, stmWorksheet):
        self.dataModelName = dataModelName
        self.tables = {}
        self.isStmPopulated = True
        self.stmWorksheet = stmWorksheet

        if (len(dataModelSchema) == 0):
            self.isStmPopulated = False

        for table in dataModelSchema:
            self.tables[table] = Table(table,
                                       dataModelSchema[table])

    def getSqlCreateStatements(self):

        dataModelCreateStatements = []
        for tableName in self.tables:
            dataModelCreateStatements.append(self.tables[tableName]
                                             .getSqlCreateStatements())

        return dataModelCreateStatements

    def __str__(self):
        tablesStr = '\n' + '  ** ' + self.dataModelName + ' **' + '\n'
        if (self.isStmPopulated):
            for tableName in self.tables:
                tablesStr += str(self.tables[tableName])
        else:
            tablesStr += '\n' + '      !! DATA MODEL MISSING FROM STM !!'     \
                              + '\n'

        return tablesStr


class SrcLayer():
    # 1+ Data Models, one for each source system
    # {sourceSystemId:DataModel}

    def __init__(self):

        log.debug("START")

        # The SRC Layer object holds the data model schema and the
        # source system connection details
        self.dataModels = {}
        self.srcSystemConns = {}  # {<dataModelId>:Connection()}
        self.getSrcSysDBConnections()
        self.loadLogicalDataModel()

    def loadLogicalDataModel(self):
        # Pull schemas for each data model out of the worksheets, and
        # bulid up a dictionary of DataModels > Tables > [{column metadata}],
        # but just pass this through to the "child" constructors. We'll leave
        # this SrcLayer object with just a simple reference to a dictionary
        # of DataModel objects, and let these "child" objects handle the detail

        log.debug("START")

        tempSchema = {}

        srcWorksheets = utils.getStmWorksheets(dataLayer='src')
        for srcWorksheet in srcWorksheets:

            # Cut off the "ETL.SRC." prefix
            dataModelId = srcWorksheet.title[srcWorksheet.title.rfind('.')+1:]

            # Each worksheet is one data model - corresponding to the entire
            # SRC for one source system.
            # Create a new entry in our dataModel dictionary, indexed by the
            # source system ID

            tempSchema[dataModelId] = {}

            # Get the schema for this DataModel
            stmRows = srcWorksheet.get_all_records()

            # We've got a list of cols, so detect when we move to a new table
            for stmRow in stmRows:
                if stmRow['Table Name'].lower() not in tempSchema[dataModelId]:
                    # Insert a new table into our dictionary for this data
                    # model, indexed by table name, with an empty list of cols
                    tempSchema[dataModelId][stmRow['Table Name'].lower()] = []

                # And add the current column to this table's list of columns.
                # Each list item is a dictionary of column metadata indexed by
                # column name
                tmpList = tempSchema[dataModelId][stmRow['Table Name'].lower()]
                tmpList.append({'columnName': stmRow['Column Name'],
                                'dataType': stmRow['data_type'],
                                'isNK': stmRow['natural_key'],
                                'isAudit': stmRow['audit_column']})

            # Create the DataModel() for this SRC system. This is the object
            # we "leave behind" in this class - the full schema gets passed
            # down to "child" constructors
            self.dataModels[dataModelId] = DataModel(srcWorksheet.title,
                                                     tempSchema[dataModelId],
                                                     srcWorksheet)

            log.info("Loaded logical data model for " + srcWorksheet.title)

    def getSrcSysDBConnections(self):

        log.debug("START")

        srcWorksheets = utils.getStmWorksheets(dataLayer='src')
        for srcWorksheet in srcWorksheets:
            # Cut off the SRC.<dataModelId>. prefix
            srcSysId = srcWorksheet.title[srcWorksheet.title.rfind('.')+1:]
            # Get the srcSystemConn details from settings and dump them into
            # our dictionary
            self.srcSystemConns[srcSysId] = Connection(conf.SOURCE_SYSTEM_CONNS
                                                       [srcSysId])

        log.info("Connected to source systems")

    def dropAllTables(self):

        log.debug("START")

        etlDBCursor = conf.ETL_DB_CONN.cursor()

        etlDBCursor.execute("SELECT * FROM information_schema.tables t " +
                            "WHERE t.table_schema = 'public' " +
                            "AND t.table_name LIKE '" + 'src' + "_%'")
        stgSRCtables = etlDBCursor.fetchall()

        counter = 0
        for stgSRCtable in stgSRCtables:
            try:
                etlDBCursor.execute("DROP TABLE " + stgSRCtable[2])
                conf.ETL_DB_CONN.commit()
                counter += 1
            except psycopg2.Error as e:
                pprint.pprint(e)
                # to do, need to catch if table didn't already exist
                # (expected exception), and raise everything else
                pass

        log.info("Dropped " + str(counter) + ' tables')

    def autoPopulateLogicalDataModels(self, dataModelId):

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

            counter = 0
            prevTableName = ''
            currentTableName = ''
            for row in srcColumns:
                currentTableName = row[2]

                if currentTableName != prevTableName and counter != 0:

                    # if we've added the last column of a table, before
                    # moving on, add in the audit columns
                    # These are columns we wont find in the source system, but
                    # we want to add to all tables in our ETL.STAGING.SRC data
                    # layer)
                    if prevTableName != '':
                        # I.e. if we haven't just started# our first table
                        auditCols = getAuditColumns(dataModelId,
                                                    srcSysType,
                                                    'src_' + dataModelId
                                                    + '_' + prevTableName)
                        schema.extend(auditCols)

                newCol = [dataModelId,
                          self.srcSystemConns[dataModelId].type,
                          'src_' + dataModelId + '_' + currentTableName,
                          row[3], row[6], row[7], row[8]]
                schema.append(newCol)

                counter += 1
                prevTableName = currentTableName

            # Put the audit columns onto the last table
            # (not done in the loop above)
            auditCols = getAuditColumns(dataModelId,
                                        srcSysType,
                                        'src_' + dataModelId
                                        + '_' + currentTableName)

            schema.extend(auditCols)

        elif self.srcSystemConns[dataModelId].type == 'FILESYSTEM':

            log.info("Source system type is FILESYSTEM (aka CSV): " +
                     "connecting  and pulling list of columns")

            # Get the schema from the files
            # To do: what's the max this can be? any downsides?
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
                    schema.append([dataModelId,
                                   srcSysType,
                                   'src_' + dataModelId + '_'
                                   + filename,
                                   csvColumn,
                                   'YES',
                                   'character varying',
                                   128])
                    # To do: can't just stick type as 128 chars! What's the
                    # better type here, to capture string of any length from
                    # CSV? Of course, I would expect to alter them in
                    # spreadsheet during dev

                # Add on our metadata columns (these are columns we wont
                # find in the source system, but we want to add to all tables
                # in our ETL.STAGING.SRC data layer)
                schema.extend(getAuditColumns(dataModelId,
                                              srcSysType,
                                              'src_' + dataModelId + '_'
                                              + filename))

        else:
            raise ValueError("Failed to rebuild SRC Layer: Source system " +
                             "type is ' + self.srcSystemConns[dataModelId]." +
                             "type + '. Stopping execution, because we only " +
                             "deal with 'POSGRES' and 'FILESYSTEM', so " +
                             "cannot populate the STM for this source system")

        # Check we managed to find some kind of schema
        # from the source system, otherwise abort
        if (len(schema) == 0):
            raise ValueError("Failed to rebuild SRC Layer: STM is not yet " +
                             "populated, so we tried to pull the schema " +
                             "from the source system, but we could not " +
                             "find anything")

        # We build up our new GSheets table first, in memory,
        # then write it all in one go.

        cell_list = self.dataModels[dataModelId].stmWorksheet.          \
            range('A2:G'+str(len(schema)+1))
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

        self.dataModels[dataModelId].stmWorksheet.update_cells(cell_list)

        log.info("STM schema updated in worksheet: "
                 + self.dataModels[dataModelId].stmWorksheet.title)

    def rebuildPhsyicalDataModel(self):

        log.info("START (src)")

        # First, we need to drop every staging table prefixed SRC_  - we
        # are clearing out and starting again
        log.info("Dropping all SRC tables")
        self.dropAllTables()

        haveWeChangedStm = False

        for dataModelId in self.dataModels:

            # If this worksheet of the STM is not already populated, we
            # populate it with an exact copy of the source DB's schema

            if (self.dataModels[dataModelId].isStmPopulated):
                log.info("the STM for source system <" + dataModelId + "> " +
                         "is already populated")
            else:
                log.info("the STM for source system <" + dataModelId + "> " +
                         "is NOT yet populated")
                haveWeChangedStm = True
                self.autoPopulateLogicalDataModels(dataModelId)

        if haveWeChangedStm:
            # Finally, having just updated the spreadsheet, we do a complete
            # reload of the Data Layers, so our Data Layer objects are all
            # up to date
            log.info("Reloading the logical data model from the spreadsheet")
            self.loadLogicalDataModel()

        #
        # The STM is now populated, either with a direct copy of the source
        # schema, or with a bespoke configuration entered/edited directly in
        # the spreadsheet. We now need to clear out any existing SRC staging
        # tables and recreate
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
                pass

        log.info("Created " + str(counter) + ' tables')

    def getSqlCreateStatements(self):
        log.debug("START")
        dataLayerCreateStatements = []
        for dataModelId in self.dataModels:
            dataLayerCreateStatements.extend(
                self.dataModels[dataModelId].getSqlCreateStatements())

        return dataLayerCreateStatements

    def __str__(self):
        dataModelStr = '\n' + '*** Data Layer: Source ***' + '\n'
        for dataModelId in self.dataModels:
            dataModelStr += str(self.dataModels[dataModelId])

        return dataModelStr


class StgLayer():
    # 0+ Data Models, one for each peristent "step" in the ETL's
    # transformation process
    pass


class TrgLayer():
    # 1 Data Model
    pass


class SumLayer():
    # 1 Data Model
    pass


def getAuditColumns(dataModelId, dataModelType, tableName):
    log.debug("START")
    schema = []
    schema.append([dataModelId,
                   dataModelType,
                   tableName,
                   'audit_source_system', 'NO', 'text', ''])
    schema.append([dataModelId,
                   dataModelType,
                   tableName,
                   'audit_bulk_load_date', 'NO', 'date', ''])
    schema.append([dataModelId,
                   dataModelType,
                   tableName,
                   'audit_latest_delta_load_date', 'YES', 'date', ''])
    schema.append([dataModelId,
                   dataModelType,
                   tableName,
                   'audit_latest_delta_load_operation', 'YES', 'text', ''])
    log.debug("END")
    return schema


def getSrcLayerSchema():
    global SRC_LAYER
    return SRC_LAYER
