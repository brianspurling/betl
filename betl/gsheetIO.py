import pandas as pd


def readDataFromWorksheet(worksheet, testDataLimit=None):

    data = worksheet.get_all_values()
    if testDataLimit is not None:
        rowLimit = len(data)
    else:
        rowLimit = testDataLimit

    return pd.DataFrame(data[1:rowLimit], columns=data[0])
