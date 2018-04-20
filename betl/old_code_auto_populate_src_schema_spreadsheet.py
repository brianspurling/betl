
    def autoPopulateSrcSchemaGSheet(self, srcDataLayer):

        gsheet = srcDataLayer.schemaGSheetConn
        dataModelSchemas = {}

        for dataModelID in srcDataLayer.dataModels:
            dataModel = srcDataLayer.dataModels[dataModelID]
            srcSysConn = dataModel.srcSysConn
            dataModelSchemas[dataModelID] = \
                self.getDataModelSchemaFromSrcSystem(dataModelID,
                                                     srcSysConn)

        for dataModelID in dataModelSchemas:
            tableSchemas = dataModelSchemas[dataModelID].tableSchemas
            for tableName in tableSchemas:
                colSchemas = tableSchemas[tableName].columnSchemas

                wsName = 'ETL.SRC.' + dataModelID + '.' + tableName

                gsheet.del_worksheet(wsName)
                ws = gsheet.add_worksheet(title=wsName,
                                          rows=len(colSchemas)+1,
                                          cols=3)

                # We build up our new GSheets table first, in memory,
                # then write it all in one go.

                cell_list = ws.range('A2:C'+str(len(len(colSchemas))+1))
                rangeRowCount = 0
                for col in colSchemas:
                    cell_list[rangeRowCount].value = colSchemas[col].columnName
                    cell_list[rangeRowCount+1].value = colSchemas[col].dataType
                    cell_list[rangeRowCount+2].value = \
                        colSchemas[col].columnType
                    rangeRowCount += 3
                ws.update_cells(cell_list)

    # We have one data model (in the SRC data layer) for every src system
    def getDataModelSchemaFromSrcSystem(self, dataModelID, srcSysConn):

        # We run this for only one dataModel at a time
        dataModelSchema = {
            'dataModelID': dataModelID,
            'tableSchemas': {}
        }

        if srcSysConn.datastoreType == 'POSTGRES':
            # one row in information_schema.columns for each column, spanning
            # multiple tables
            dbCursor = srcSysConn.conn.cursor()
            dbCursor.execute("SELECT * FROM information_schema.columns c " +
                             "WHERE c.table_schema = 'public' " +
                             "ORDER BY c.table_name")
            postgresSchema = dbCursor.fetchall()
            previousTableName = ''
            colSchemasFromPG = []
            for colSchemaFromPG in postgresSchema:
                currentTableName = ('src_' + dataModelID + '_' +
                                    colSchemaFromPG[2])
                if currentTableName == previousTableName:
                    colSchemasFromPG.append(colSchemaFromPG)
                else:
                    dataModelSchema.tableSchemas[previousTableName] = \
                        self.convertPostgresSrcSysSchemaToColSchemas(
                            previousTableName,
                            colSchemasFromPG)
                    colSchemasFromPG = []
                    colSchemasFromPG.append(colSchemaFromPG)
                    previousTableName = currentTableName

        elif srcSysConn.datastoreType == 'FILESYSTEM':
            # one DataModel has 1+ files, each with a list of cols in
            # the first row
            # NOTE: this should be using the standard reader funcs from fileIO
            # NOTE: what is the best way to handle the csv field size?
            csv.field_size_limit(1131072)
            for filename in srcSysConn.files:
                with open(filename + '.csv', "r") as srcFile:
                    delimiter = srcSysConn.files[filename]['delimiter']
                    quoteChar = srcSysConn.files['quotechar']
                    csvReader = csv.reader(srcFile,
                                           delimiter=delimiter,
                                           quotechar=quoteChar)
                dataModelSchema.tableSchemas[filename] = \
                    self.convertFileSrcSysSchemaToColSchemas(filename,
                                                             next(csvReader))

        else:
            raise ValueError("Failed to rebuild SRC Layer: Source system " +
                             "type is " +
                             srcSysConn.datastoreType +
                             ". Stopping execution. We only " +
                             "deal with 'POSTGRES' and 'FILESYSTEM' source " +
                             "system types, so cannot auto-populate the " +
                             "ETL.SRC schemas for this source system")

        # Check we managed to find some kind of schema from the source system
        if (len(dataModelSchema.tableSchemas) == 0):
            raise ValueError("Failed to rebuild SRC Layer: SRC data layer " +
                             "in the ETL DB schema spreadsheet is not yet " +
                             "populated, so we tried to pull the schema " +
                             "from the source systems directly, but we " +
                             "could not find anything")

        return dataModelSchema

    def convertPostgresSrcSysSchemaToColSchemas(self,
                                                tableName,
                                                colSchemasFromPG):

        colSchemas = {}

        for colSchemaFromPG in colSchemasFromPG:
            colName = colSchemaFromPG[3]

            colSchemas[colName] = {
                'tableName': tableName,
                'columnName': colName,
                'dataType': colSchemaFromPG[7],
                'columnType': 'Attribute',
                'fkDimension': None
            }

        tableSchema = {
            'tableName': tableName,
            'columnSchemas': colSchemas
        }

        return tableSchema

    def convertFileSrcSysSchemaToColSchemas(self,
                                            tableName,
                                            colSchemasFromFile):

        colSchemas = {}

        for colName in colSchemasFromFile:
            colSchemas[colName] = {
                'tableName': tableName,
                'columnName': colName,
                'dataType': 'TEXT',  # NOTE: why am I not pulling this straight
                                     # from DB?
                'columnType': 'Attribute',
                'fkDimension': None
            }

        tableSchema = {
            'tableName': tableName,
            'columnSchemas': colSchemas
        }

        return tableSchema
