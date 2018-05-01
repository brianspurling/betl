import pandas as pd


def readDataFromWorksheet(worksheet, limitdata=None):

    data = worksheet.get_all_values()
    if limitdata is not None:
        rowLimit = len(data)
    else:
        rowLimit = limitdata

    return pd.DataFrame(data[1:rowLimit], columns=data[0])
