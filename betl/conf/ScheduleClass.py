class Schedule():

    def __init__(self, scheduleConfig):
        self.DEFAULT_EXTRACT = scheduleConfig['DEFAULT_EXTRACT']
        self.DEFAULT_LOAD = scheduleConfig['DEFAULT_LOAD']
        self.DEFAULT_SUMMARISE = scheduleConfig['DEFAULT_SUMMARISE']
        self.DEFAULT_DM_DATE = scheduleConfig['DEFAULT_DM_DATE']
        self.DEFAULT_DM_AUDIT = scheduleConfig['DEFAULT_DM_AUDIT']
        self.EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT = \
            scheduleConfig['EXT_TABLES_TO_EXCLUDE_FROM_DEFAULT_EXT']
        self.BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD = \
            scheduleConfig['BSE_TABLES_TO_EXCLUDE_FROM_DEFAULT_LOAD']
        self.EXTRACT_DATAFLOWS = scheduleConfig['EXTRACT_DATAFLOWS']
        self.TRANSFORM_DATAFLOWS = scheduleConfig['TRANSFORM_DATAFLOWS']
        self.LOAD_DATAFLOWS = scheduleConfig['LOAD_DATAFLOWS']
        self.SUMMARISE_DATAFLOWS = scheduleConfig['SUMMARISE_DATAFLOWS']
