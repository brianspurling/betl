from datetime import datetime


def setAuditCols(self, dataset, bulkOrDelta, sourceSystem, desc):

    self.stepStart(desc=desc)

    if bulkOrDelta.upper() == 'BULK':
        self.data[dataset]['audit_source_system'] = sourceSystem
        self.data[dataset]['audit_bulk_load_date'] = datetime.now()
        self.data[dataset]['audit_latest_delta_load_date'] = None
        self.data[dataset]['audit_latest_load_operation'] = 'BULK'

    report = ''

    self.stepEnd(report=report)


def createAuditNKs(self, dataset, desc):

    self.stepStart(desc=desc)

    # TODO will improve this over time. Basic solution as PoC
    self.data[dataset]['nk_audit'] = \
        self.data[dataset]['audit_latest_load_operation'] + '_' + '10'

    self.data[dataset].drop(
        ['audit_source_system',
         'audit_bulk_load_date',
         'audit_latest_delta_load_date',
         'audit_latest_load_operation'],
        axis=1,
        inplace=True)

    report = ''

    self.stepEnd(report=report)
