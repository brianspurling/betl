import pprint

from . import dbIO


def generateExeSummary(conf, execId, bulkOrDelta, limitedData):

    if limitedData is None:
        limitedData = 'FALSE'
    else:
        limitedData = 'TRUE'

    sql = ''
    sql += "select e.exec_id as exec_id, "
    sql += "	   e.start_datetime as exec_start, "
    sql += "	   e.end_datetime as exec_end, "
    sql += "	   e.status as exec_status, "
    sql += "	   e.bulk_or_delta as bulk_or_delta, "
    sql += "	   e.limited_data as limited_data, "
    sql += "	   f.stage as stage, "
    sql += "	   f.function_name as function_name, "
    sql += "	   f.sequence as function_sequence, "
    sql += "       f.status as function_status, "
    sql += "       f.start_datetime as function_start, "
    sql += "       f.end_datetime as function_end, "
    sql += "       d.description as dataflow_description, "
    sql += "       d.status as dataflow_status, "
    sql += "       d.row_count as dataflow_row_count, "
    sql += "       d.col_count as dataflow_col_count, "
    sql += "       d.start_datetime as dataflow_start, "
    sql += "       d.end_datetime as dataflow_end, "
    sql += "       s.description as step_description, "
    sql += "       s.status as step_status, "
    sql += "       s.row_count as step_row_count, "
    sql += "       s.col_count as step_col_count, "
    sql += "       s.start_datetime as step_start, "
    sql += "       s.end_datetime as step_end "
    sql += "from   executions e "
    sql += "left join functions f on f.exec_id = e.exec_id "
    sql += "left join dataflows d on d.function_id = f.function_id "
    sql += "left join steps s on s.dataflow_id = d.dataflow_id "
    sql += "where  e.exec_id <= " + str(execId) + " "
    sql += "and    e.bulk_or_delta = '" + bulkOrDelta + "' "
    sql += "and    e.limited_data = " + limitedData + " "
    sql += "order by exec_id desc, "
    sql += "         f.sequence asc, "
    sql += "         d.dataflow_id asc, "
    sql += "         s.step_id asc "

    df = dbIO.customSQL(
        sql=sql,
        datastore=conf.CTRL.CTRL_DB.datastore)
