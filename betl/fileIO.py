import pandas as pd
import os


def writeDataToCsv(conf, df, path, filename, headers, mode):

    _filename = ''

    if filename in conf.state.fileNameMap:
        _filename = conf.state.fileNameMap[filename]
    else:
        prefix = \
            str(conf.state.nextFilePrefix).zfill(conf.state.filePrefixLength)
        _filename = prefix + "-" + filename
        conf.state.nextFilePrefix += 1
        conf.state.fileNameMap[filename] = _filename

    _file = open(path + _filename, mode)

    # If we're appending, we never put the column headers in
    colHeaders = headers
    if mode == 'a':
        colHeaders = None

    df.to_csv(_file, header=colHeaders, index=False)


def truncateFile(conf, path, filename):

    _filename = ''
    if filename in conf.state.fileNameMap:
        _filename = conf.state.fileNameMap[filename]
        if os.path.exists(path + _filename):
            _file = open(path + _filename, 'w')
            _file.close()


def readDataFromCsv(conf,
                    path,
                    filename,
                    sep=',',
                    quotechar='"',
                    nrows=None,
                    isTmpData=True,
                    limitdata=None,
                    getFirstRow=False):

    _filename = filename
    if isTmpData:
        _filename = conf.state.fileNameMap[filename]

    # We need to force it to read everything as text. Only way I can
    # see to do this is to read the headers and setup a dtype for each
    headersDf = pd.read_csv(path + _filename,
                            sep=sep,
                            quotechar=quotechar,
                            nrows=1,
                            na_filter=False)

    headerList = headersDf.columns.values
    dtype = {}
    for header in headerList:
        dtype[header] = str

    if limitdata is not None:
        nrows = limitdata
    else:
        nrows = None
    if getFirstRow:
        nrows = 1

    return pd.read_csv(path + _filename,
                       sep=sep,
                       quotechar=quotechar,
                       dtype=dtype,
                       na_filter=False,
                       nrows=nrows)
