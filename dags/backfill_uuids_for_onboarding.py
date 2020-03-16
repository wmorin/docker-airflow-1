
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable



params = {}


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
        'STATS_DB_PORT':  Variable.get('STATS_DB_PORT'),
        'STATS_DB_HOST': Variable.get('STATS_DB_HOST'),
        'STATS_DB_NAME': Variable.get('STATS_DB_NAME'),
        'STATS_DB_USERNAME': Variable.get('STATS_DB_USERNAME'),
        'STATS_DB_PASSWORD': Variable.get('STATS_DB_PASSWORD'),
        'ANALYTICS_DB_PORT': Variable.get('ANALYTICS_DB_PORT'),
        'ANALYTICS_DB_HOST': Variable.get('ANALYTICS_DB_HOST'),
        'ANALYTICS_DB_NAME': Variable.get('ANALYTICS_DB_NAME'),
        'ANALYTICS_DB_USERNAME': Variable.get('ANALYTICS_DB_USERNAME'),
        'ANALYTICS_DB_PASSWORD': Variable.get('ANALYTICS_DB_PASSWORD')}


