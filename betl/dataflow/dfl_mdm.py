import pandas as pd
from betl.logger import alerts


# TODO: I can't stop eg. '16 ' being converted to '16' when it gets
# written to Sheets, which can created dupes in the mapping cols.
# So trim in app before calling this :(
def mapMasterData(self,
                  dataset,
                  mdmWS,
                  joinCols,
                  masterDataCols,
                  desc,
                  autoPopMappingCols=[]):

    self.stepStart(desc=desc)

    ############################
    # Connect to MDM Worksheet #
    ############################

    ws = self.conf.DATA.getMDMDatastore().conn.worksheet(mdmWS)

    #################################
    # Extract the current MDM table #
    #################################

    mdm_list = ws.get_all_values()

    if(len(mdm_list)) == 0:
        raise ValueError('MDM column headings must be entered into the ' +
                         'Google Sheet')
    if(len(mdm_list)) == 1:
        df_mdm = pd.DataFrame(columns=mdm_list[0:1][0])
    else:
        df_mdm = pd.DataFrame(mdm_list[1:], columns=mdm_list[0:1][0])

    #####################
    # Fill NaNs in data #
    #####################

    # NaNs won't join to empty cells in our Gsheet, so we replace
    self.data[dataset] = self.data[dataset].fillna(value='')

    ##########################
    # Rename autoPop columns #
    ##########################

    # We might have masterDataCols in our dataset, so we will rename
    # them before we join to allow us to control them later
    # Note that we assume any autoPop map columns have been joined to
    # the main dataset with the same logic the MDM will be using (i.e.
    # the join columns). Which would mean that you couldn't have
    # two identical joinCol combos with different autoPopMapCols.
    # This is important, because we will group below before writing back
    # to the MDM spreadsheet, and we'll get multiple rows
    # where we don't want them if this is not the case.
    renamedAutoPopColNames = []
    for colName in autoPopMappingCols:
        autoPopColName = 'autoPop_' + colName
        self.data[dataset].rename(index=str,
                                  columns={colName: autoPopColName},
                                  inplace=True)
        renamedAutoPopColNames.append(autoPopColName)

    ##########################
    # Merge the two datasets #
    ##########################

    df_m = pd.merge(
        self.data[dataset],
        df_mdm,
        on=joinCols,
        how='outer',
        indicator=True)

    #################
    # Auto Populate #
    #################

    # AutoPop does not overwrite data already in the MDM spreadsheet

    for autoPopCol in renamedAutoPopColNames:
        mapCol = autoPopCol[8:]
        df_m.loc[df_m[mapCol].isnull(), mapCol] = \
            df_m[df_m[mapCol].isnull()][autoPopCol]
        df_m.loc[df_m[mapCol] == '', mapCol] = \
            df_m[df_m[mapCol] == ''][autoPopCol]

    numOfRowsWithMatchingMDM = \
        len(df_m.loc[df_m['_merge'] == 'both'].index)
    numOfRowsWithoutMatchingMDM = \
        len(df_m.loc[df_m['_merge'] == 'left_only'].index)
    numOfUnmatchedRowsInMDM = \
        len(df_m.loc[df_m['_merge'] == 'right_only'].index)

    #########################
    # Drop unneeded columns #
    #########################

    if 'count' in list(df_m):
        colsToDrop = ['count'] + renamedAutoPopColNames
    else:
        colsToDrop = renamedAutoPopColNames
    df_m.drop(
        labels=colsToDrop,
        axis=1,
        inplace=True)

    ###################################
    # Replace NaNs with empty strings #
    ###################################

    # If the mapping was missing, we treat it as though it was a blank
    # cell in the Gsheet
    df_m[masterDataCols] = df_m[masterDataCols].fillna(value='')

    ###############################
    # Update our output dataframe #
    ###############################

    self.data[dataset] = df_m.loc[df_m['_merge'] != 'right_only'].copy()
    self.data[dataset].drop(
        labels='_merge',
        axis=1,
        inplace=True)

    ######################################################
    # Prepare new MDM dataset for write back (to gsheet) #
    ######################################################

    # Drop any additional columns (not part of the MDM dataset)
    colsToDrop = list(df_m)
    colsToDrop = \
        [x for x in list(df_m) if x not in joinCols + masterDataCols]
    df_m.drop(
        labels=colsToDrop,
        axis=1,
        inplace=True)

    # Group up to distinct set of rows
    df_m = df_m.groupby(
        df_m.columns.tolist(),
        sort=False,
        as_index=False).size().reset_index(name='count')
    df_m.sort_values(by='count', ascending=False, inplace=True)

    ###############################
    # Count up empty mapping rows #
    ###############################

    numOfMDMRowsWithNoFilledInValues = 0
    for i, row in df_m.iterrows():
        rowIsAllBlank = True
        for colName in masterDataCols:
            if row[colName] != '':
                rowIsAllBlank = False
        if rowIsAllBlank:
            numOfMDMRowsWithNoFilledInValues += 1

    ####################
    # Write to GSheets #
    ####################

    colsToWriteBack = joinCols + masterDataCols + ['count']
    ws.resize(rows=len(df_m)+1, cols=len(colsToWriteBack))
    cell_list = ws.range(1, 1, len(df_m)+1, len(colsToWriteBack))
    cellPos = 0
    for colName in colsToWriteBack:
        cell_list[cellPos].value = colName
        cellPos += 1
    for i, row in df_m.iterrows():
        for colName in colsToWriteBack:
            value = str(row[colName])
            if value == '£$%^NOTAVALUE^%$£':
                value = ''
            cell_list[cellPos].value = value
            cellPos += 1
    ws.clear()
    ws.update_cells(cell_list)

    ###########
    # Wrap Up #
    ###########

    r = ''

    r += ('For MDM ' + mdmWS + ' there were \n'
          '       - ' + str(numOfRowsWithoutMatchingMDM) + ' rows in ' +
          'the data without a corresponding MDM row (these have now ' +
          'been added)\n' +
          '       - There are now ' +
          str(numOfMDMRowsWithNoFilledInValues) + ' rows in the MDM ' +
          'without any mapped values filled in\n')
    if (numOfRowsWithoutMatchingMDM > 0 or
            numOfMDMRowsWithNoFilledInValues > 0):
        alerts.logAlert(self.conf, r)
    r += ('       - ' + str(numOfRowsWithMatchingMDM) + ' rows in the ' +
          'data were matched to corresponding MDM rows, and\n' +
          '       - ' + str(numOfUnmatchedRowsInMDM) + ' rows in the ' +
          'MDM did not find a match in the data')

    if len(self.data[dataset]) == 0:
        self.stepEnd(
            report=r,
            datasetName=dataset,
            shapeOnly=False)
    else:
        self.stepEnd(
            report=r,
            datasetName=dataset,
            df=self.data[dataset],
            shapeOnly=False)
