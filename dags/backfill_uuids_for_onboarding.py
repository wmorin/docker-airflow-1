
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.dateutil import NDaysWRTToday, dateToStr
from datetime import datetime, timedelta
from utils.email_helper import email_notify


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 4),
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}

dag = DAG('backfill_uuids_for_onboarding',
          default_args=default_args,
          schedule_interval='10 02 * * 1-7',
          )


NUM_ANALYTICS_ROWS = 200


def get_analytics_stats_conn_cursors():
    analytics_conn = PostgresHook(postgres_conn_id='ANALYTICS_DB').get_conn()
    stats_conn = PostgresHook(postgres_conn_id='STATS_DB').get_conn()

    analytics_server_cursor = analytics_conn.cursor("analytics_server_cursor")
    # providing an argument makes this a server cursor, which wouldn't hold
    # all records in the memory
    # https://www.psycopg.org/docs/usage.html#server-side-cursors
    stats_client_cursor = stats_conn.cursor()
    return (analytics_conn, analytics_server_cursor, stats_conn, stats_client_cursor)


def close_conns_cursors(conns_cursors):
    for c in conns_cursors:
        c.close()


def backfill_uuids():
    (analytics_conn, analytics_server_cursor, stats_conn, stats_client_cursor) = get_analytics_stats_conn_cursors()
    filter_fconditions = ' and '.join([
        f"date between '{dateToStr(NDaysWRTToday(-1))}' and  '{dateToStr(NDaysWRTToday(0))}'"
    ])
    filters_for_update = f"and {filter_fconditions}"
    analytics_server_cursor.execute("select uuid, device_id from customer_ids_mapping;")
    while True:
        rows = analytics_server_cursor.fetchmany(NUM_ANALYTICS_ROWS)
        if not rows:
            break
        for row in rows:
            query = f"""UPDATE customer_events
                        SET uuid = %s
                        WHERE device_id = %s {filters_for_update}"""
            print(query)
            stats_client_cursor.execute(query, [row[0], row[1]])
            stats_conn.commit()

    close_conns_cursors((analytics_conn, analytics_server_cursor, stats_conn, stats_client_cursor))


t0 = PythonOperator(
    task_id='backfill_uuids',
    python_callable=backfill_uuids,
    dag=dag)