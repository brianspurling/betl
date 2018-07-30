import pandas as pd


def readDataFromWorksheet(worksheet, limitdata=None):

    data = worksheet.get_all_values()
    if limitdata is not None:
        rowLimit = limitdata
    else:
        rowLimit = len(data)

    return pd.DataFrame(data[1:rowLimit], columns=data[0])
