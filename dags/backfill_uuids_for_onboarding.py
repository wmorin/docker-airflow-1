
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from dags.utils.db_util import connect, run_query


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
          schedule_interval='00 30 02 * * 1-7',
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
        'password': Variable.get('ANALYTICS_DB_PASSWORD')}

analytics_connection = connect(**analytics_db_config)
stats_connection = connect(**stats_db_config)

# Fetch uuid, device_id mapping from analytics DB
t0 = PythonOperator(
    task_id='read_customer_ids_map',
    python_callable=run_query,
    op_args=[analytics_connection, 'select uuid, device_id from customer_ids_mapping'],
    dag=dag)

# Using the mapping fetched above, back fill uuids in customer_events table in stats DB

