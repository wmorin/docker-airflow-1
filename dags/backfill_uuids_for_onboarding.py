
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from utils.db_util import connect, run_query


params = {}  # TODO (Akshay) start and end date

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

dag = DAG('backfill_uuids_for_onboarding',
          default_args=default_args,
          schedule_interval='00 10 02 * * 1-7',
          params=params)

env = {
    'ENVIRONMENT': Variable.get('ENVIRONMENT'),
}
stats_db_config = {
    'port':  Variable.get('STATS_DB_PORT'),
    'host': Variable.get('STATS_DB_HOST'),
    'dbname': Variable.get('STATS_DB_NAME'),
    'user': Variable.get('STATS_DB_USERNAME'),
    'password': Variable.get('STATS_DB_PASSWORD'),
}
analytics_db_config = {
    'port': Variable.get('ANALYTICS_DB_PORT'),
    'host': Variable.get('ANALYTICS_DB_HOST'),
    'dbname': Variable.get('ANALYTICS_DB_NAME'),
    'user': Variable.get('ANALYTICS_DB_USERNAME'),
    'password': Variable.get('ANALYTICS_DB_PASSWORD')
}




def backfill_uuids(**context):
    analytics_cursor = context['task_instance'].xcom_pull(task_ids='read_customer_ids_map')
    stats_connection = context['task_instance'].xcom_pull(task_ids='connect_stats')
    for row in analytics_cursor:
        uuid, device_id = row[0], row[1]
        query = """insert into customer_events(uuid)
                   values(%s) where device_id = %s
                   on conflict(uuid) do nothing;
                """   # TODO (Akshay) include start and end dates in where clause
        run_query(stats_connection, query, [uuid, device_id])


def get_uuid_device_id_mapping(op_args, **context):
    query = op_args[0]
    analytics_connection = context['task_instance'].xcom_pull(task_ids='connect_analytics')
    return run_query(analytics_connection, query)

#Get analytics connection
t0 = PythonOperator(
    task_id='connect_analytics',
    python_callable=connect,
    op_args=[analytics_db_config],
    dag=dag)

# Fetch uuid, device_id mapping from analytics DB
t1 = PythonOperator(
    task_id='read_customer_ids_map',
    python_callable=get_uuid_device_id_mapping,
    provide_context = True,
    op_args=['select uuid, device_id from customer_ids_mapping;'],
    dag=dag)

#Get stats connection
t2 = PythonOperator(
    task_id='connect_stats',
    python_callable=connect,
    op_args=[stats_db_config],
    dag=dag)

# Using the mapping fetched above, back fill uuids in customer_events table in stats DB
t3 = PythonOperator(
    task_id='backfill_uuids',
    python_callable=backfill_uuids,
    provide_context=True,
    dag=dag)


t0 >> t1 >> t2 >> t3
