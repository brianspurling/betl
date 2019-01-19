import pandas as pd
import os


def readDataFromCsv(fileNameMap,
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
        _filename = fileNameMap[filename]

    # We need to force it to read everything as text. Only way I can
    # see to do this is to read the headers and create a dtype for each
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


def writeDataToCsv(conf, df, path, filename, headers, mode):

    _filename = ''

    if filename in CONF.FILE_NAME_MAP:
        _filename = CONF.FILE_NAME_MAP[filename]
    else:
        prefix = \
            str(CONF.NEXT_FILE_PREFIX).zfill(CONF.FILE_PREFIX_LENGTH)
        _filename = prefix + "-" + filename
        CONF.NEXT_FILE_PREFIX += 1
        CONF.FILE_NAME_MAP[filename] = _filename

    _file = open(path + _filename, mode)

    # If we're appending, we never put the column headers in
    colHeaders = headers
    if mode == 'a':
        colHeaders = None

    df.to_csv(_file, header=colHeaders, index=False)


def truncateFile(conf, path, filename):

    _filename = ''
    if filename in CONF.FILE_NAME_MAP:
        _filename = CONF.FILE_NAME_MAP[filename]
        if os.path.exists(path + _filename):
            _file = open(path + _filename, 'w')
            _file.close()
