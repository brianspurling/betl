def exampleDataflow(betl):

    dfl = betl.DataFlow(desc='Example dataflow')

    dfl.read(
        tableName='example_table_name',
        dataLayer='EXT')

    dfl.dedupe(
        dataset='example_table_name',
        desc='Make dataset unique')

    dfl.write(
        dataset='example_table_name',
        targetTableName='trg_dm_example',
        dataLayerID='TRN')
