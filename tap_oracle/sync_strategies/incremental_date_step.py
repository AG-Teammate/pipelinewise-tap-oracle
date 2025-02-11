#!/usr/bin/env python3
import singer
from singer import utils, write_message, get_bookmark
import singer.metadata as metadata
from singer.schema import Schema
import tap_oracle.db as orc_db
import tap_oracle.sync_strategies.common as common
import singer.metrics as metrics
import copy
import pdb
import time
import decimal
import cx_Oracle
import datetime as dt
from datetime import datetime, timedelta
import pytimeparse
import jinja2

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 1000
# An offset value that can be configured to shift the incremental filter clause
OFFSET_VALUE = 0

BATCH_SIZE = 1000

jinjaenv = jinja2.Environment(loader=jinja2.FileSystemLoader('./'))


def sync_table(conn_config, stream, state, desired_columns):
    connection = orc_db.open_connection(conn_config)
    connection.outputtypehandler = common.OutputTypeHandler
    dateformat = '%Y-%m-%dT%H:%M:%S.00+00:00'

    cur = connection.cursor()
    cur.arraysize = BATCH_SIZE
    cur.execute("ALTER SESSION SET TIME_ZONE = '00:00'")
    cur.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS."00+00:00"'""")
    cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
    cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")
    time_extracted = utils.now()

    stream_version = singer.get_bookmark(state, stream.tap_stream_id, 'version')
    # If there was no bookmark for stream_version, it is the first time
    # this table is being sync'd, so get a new version, write to
    # state
    if stream_version is None:
        stream_version = int(time.time() * 1000)
        state = singer.write_bookmark(state,
                                      stream.tap_stream_id,
                                      'version',
                                      stream_version)
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    activate_version_message = singer.ActivateVersionMessage(
        stream=stream.tap_stream_id,
        version=stream_version)
    singer.write_message(activate_version_message)

    md = metadata.to_map(stream.metadata)
    schema_name = md.get(()).get('schema-name')

    escaped_columns = list(map(lambda c: common.prepare_columns_sql(stream, c), desired_columns))
    escaped_schema = schema_name
    escaped_table = stream.table

    replication_key = md.get((), {}).get('replication-key')
    replication_key_initial_value = md.get((), {}).get('replication-key-initial-value')
    date_step_value = md.get((), {}).get('date-step-value')
    custom_query_file = md.get((), {}).get('custom-query-file')
    # escaped_replication_key = common.prepare_columns_sql(stream, replication_key)
    replication_key_sql_datatype = md.get((), {}).get('replication-key-sql-datatype')
    if replication_key_sql_datatype is None:
        replication_key_sql_datatype = md.get(('properties', replication_key)).get('sql-datatype')
    additional_where_clause = md.get((), {}).get('additional-where-clause')
    if additional_where_clause is not None:
        additional_where_clause = "AND (" + additional_where_clause + ")"
    else:
        additional_where_clause = ""

    typed_offset_value = OFFSET_VALUE
    LOGGER.info(f"{replication_key_sql_datatype=}")
    if replication_key_sql_datatype == 'DATE' or replication_key_sql_datatype.startswith('TIMESTAMP'):
        typed_offset_value = f"INTERVAL '{OFFSET_VALUE}' SECOND"

    with metrics.record_counter(None) as counter:
        rows_saved = 0

        while True:
            replication_key_value = singer.get_bookmark(state, stream.tap_stream_id, 'replication_key_value')
            step_start_d = datetime.strptime(replication_key_initial_value, dateformat)
            if replication_key_value:
                step_start_d = datetime.strptime(replication_key_value, dateformat)

            step_end_d = step_start_d + timedelta(seconds=pytimeparse.parse(date_step_value))
            LOGGER.info(
                f"Performing Incremental Date Step replication from {replication_key} = {step_start_d.strftime(dateformat)} + {typed_offset_value} to {step_end_d.isoformat()}")

            now = datetime.now()
            casted_where_clause_arg = common.prepare_where_clause_arg(step_start_d.strftime(dateformat),
                                                                      replication_key_sql_datatype)
            casted_where_clause_arg2 = common.prepare_where_clause_arg(step_end_d.strftime(dateformat),
                                                                       replication_key_sql_datatype)

            date_cond = f"{replication_key} BETWEEN {casted_where_clause_arg} AND {casted_where_clause_arg2}"
            if isinstance(replication_key, list):
                date_cond = ' OR '.join(
                    map(lambda s: f"{s} BETWEEN {casted_where_clause_arg} AND {casted_where_clause_arg2}",
                        replication_key))
            select_sql = f"""SELECT /*+ PARALLEL */ {','.join(escaped_columns)}
                                FROM {escaped_schema}.{escaped_table}
                                WHERE ({date_cond})
                                {additional_where_clause}
                                """

            if custom_query_file is not None:
                tpl = jinjaenv.get_template(custom_query_file)
                select_sql = tpl.render(additional_where_clause=additional_where_clause,
                                        escaped_columns=escaped_columns, escaped_schema=escaped_schema,
                                        escaped_table=escaped_table, date_cond=date_cond,
                                        date_start=casted_where_clause_arg, date_end=casted_where_clause_arg2)

            LOGGER.info("select %s", select_sql)
            for row in cur.execute(select_sql):
                record_message = common.row_to_singer_message(stream,
                                                              row,
                                                              stream_version,
                                                              desired_columns,
                                                              time_extracted)

                singer.write_message(record_message)
                rows_saved = rows_saved + 1

                counter.increment()

            LOGGER.info("Date step loop finished")

            if step_end_d < now:
                state = singer.write_bookmark(state,
                                              stream.tap_stream_id,
                                              'replication_key_value',
                                              step_end_d.strftime(dateformat))  # set state to the end of the interval

            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

            if step_end_d >= now:
                LOGGER.info("Date step is in the future, stopping the sync")
                break

    cur.close()
    connection.close()
    return state

# Local Variables:
# python-indent-offset: 3
# End:
