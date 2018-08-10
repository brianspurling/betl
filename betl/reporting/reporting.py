from betl.io import dbIO
from betl.logger import Logger
import plotly.graph_objs as go
import plotly.offline as py


def generateExeSummary(conf, execId, bulkOrDelta, limitedData):

    log = Logger()

    if limitedData is None:
        limitedData = 'FALSE'
    else:
        limitedData = 'TRUE'

    sql = ""
    sql += "select latest.dataflow_id, \n"
    sql += "       latest.step_id, \n"
    sql += "	   latest.step_row_count latest_row_count, \n"
    sql += "	   prev.* \n"
    sql += "from   (select e.exec_id as exec_id, \n"
    sql += "               d.dataflow_id as dataflow_id, \n"
    sql += "               s.step_id as step_id, \n"
    sql += "		       d.description as dataflow_description, \n"
    sql += "	           s.description as step_description, \n"
    sql += "	           s.row_count as step_row_count, \n"
    sql += "		       s.col_count as step_col_count, \n"
    sql += "		       s.start_datetime as step_start, \n"
    sql += "		       s.end_datetime as step_end \n"
    sql += "		from   executions e \n"
    sql += "		inner join functions f on f.exec_id = e.exec_id \n"
    sql += "		inner join dataflows d on d.function_id = f.function_id \n"
    sql += "		inner join steps s on s.dataflow_id = d.dataflow_id \n"
    sql += "		where  e.exec_id = " + str(execId) + " \n"
    sql += "		and    e.bulk_or_delta = '" + bulkOrDelta + "' \n"
    sql += "		and    e.limited_data = " + limitedData + " \n"
    sql += "		and    f.status = 'SUCCESSFUL' \n"
    sql += "		and    s.row_count is not null \n"
    sql += "		and    d.dataflow_id is not null) latest \n"
    sql += "left join (select d.description as dataflow_description, \n"
    sql += "		     	  s.description as step_description, \n"
    sql += "			      round(AVG(s.row_count)) as avg_row_count, \n"
    sql += "			      round(MAX(s.row_count)) as max_row_count, \n"
    sql += "			      round(MIN(s.row_count)) as min_row_count, \n"
    sql += "			      round(stddev_pop(s.row_count)) as std_row_count \n"
    sql += "		 	      -- s.col_count as step_col_count, \n"
    sql += "		          -- s.start_datetime as step_start, \n"
    sql += "		          -- s.end_datetime as step_end \n"
    sql += "		   from   executions e \n"
    sql += "		   inner join functions f on f.exec_id = e.exec_id \n"
    sql += "	       inner join dataflows d on d.function_id = f.function_id \n"
    sql += "		   inner join steps s on s.dataflow_id = d.dataflow_id \n"
    sql += "		   where  e.exec_id < " + str(execId) + " \n"
    sql += "		   and    e.bulk_or_delta = '" + bulkOrDelta + "' \n"
    sql += "		   and    e.limited_data = " + limitedData + " \n"
    sql += "           and    f.status = 'SUCCESSFUL' \n"
    sql += "	       and    s.row_count is not null \n"
    sql += "		   and    d.dataflow_id is not null \n"
    sql += "	       group by s.description, \n"
    sql += "			  	    d.description) prev \n"
    sql += "on  latest.dataflow_description = prev.dataflow_description  \n"
    sql += "and latest.step_description = prev.step_description \n"
    sql += "order by latest.dataflow_id asc, \n"
    sql += "         latest.step_id asc \n"

    df = dbIO.customSQL(
        sql=sql,
        datastore=conf.CTRL.CTRL_DB.datastore)

    if len(df) > 1:
        df.step_id = df.dataflow_description + ' ~ ' + df.step_description
        df.loc[(df.std_row_count == 0) &
               (df.avg_row_count != df.latest_row_count), 'variance'] = -1
        df.loc[(df.std_row_count == 0) &
               (df.avg_row_count == df.latest_row_count), 'variance'] = 0
        df.loc[df.std_row_count != 0, 'variance'] = (
            abs(df.loc[df.std_row_count != 0].avg_row_count -
                df.loc[df.std_row_count != 0].latest_row_count) /
            df.std_row_count)

        varianceLimit = 1.5
        df = df.loc[(df.variance > varianceLimit) | (df.variance == -1)]

        df['tooltips'] = (
            '<b>Variance: ' + df['variance'].map(str) + '</b> <br>' +

            'Avg: ' + df['avg_row_count'].map(str) + ' <br>' +
            'Max: ' + df['max_row_count'].map(str) + ' <br>' +
            'Min: ' + df['min_row_count'].map(str) + ' <br>' +
            'Std: ' + df['std_row_count'].map(str) + ' <br>' +
            'Latest: ' + df['latest_row_count'].map(str) + ' <br>' +
            df['step_description'].map(str))

        log.logVariancesReport()

        if len(df) == 0:
            log.logNoVariancesReported(varianceLimit)
        else:
            data = [
                go.Bar(
                    x=df['step_id'],
                    y=df['variance'],
                    text=df['tooltips'],
                    hoverinfo='text')]

            df.to_csv(conf.CTRL.REPORTS_PATH + '/exeSummary.csv')
            url = py.plot(
                data,
                filename=conf.CTRL.REPORTS_PATH + '/exeSummary.html')
            log.logSomeVariancesReported(varianceLimit, url)
