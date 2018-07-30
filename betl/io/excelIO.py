import pandas as pd


def readDataFromWorksheet(worksheet, limitdata=None):

    df = pd.DataFrame()
    maxCol = 0

    if limitdata is not None:
        rowLimit = limitdata
    else:
        rowLimit = 0

    tableData = []
    for row in worksheet.iter_rows(min_row=1, max_row=rowLimit):
        rowData = []
        colIndex = 0
        rowHasData = False
        for cell in row:
            colIndex += 1
            if cell.value is not None:
                rowHasData = True
                if colIndex > maxCol:
                    maxCol = colIndex
            rowData.append(cell.value)
        if rowHasData:
            tableData.append(rowData)
        else:
            # If we find a completely empty row, stop
            break

    df = pd.DataFrame(tableData)
    df = df.iloc[1:, 0:maxCol]
    df.columns = tableData[0][0:maxCol]
    return df
