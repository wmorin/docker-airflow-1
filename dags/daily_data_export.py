
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.airflow_helper import get_environments
from api_exports.run_exports import run_exports
from api_exports.run_exports import validate_exports


default_args = {
    'owner': 'Jaekwan',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 31),
    'email': ['swe@agentiq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

dag = DAG('daily_data_export',
          default_args=default_args,
          # run every day at 3:10am PST after conversation closure
          schedule_interval='10 3 * * 1-7')

# set environment variables
os.environ.update(get_environments())


def get_start_end_time(execution_date):
    # time might need to fine control again
    return (execution_date.subtract(days=1).format("%Y-%m-%d %H:%M:%S"),
            execution_date.format("%Y-%m-%d %H:%M:%S"))


def run_export(*args, **kwargs):
    start_time, end_time = get_start_end_time(kwargs['execution_date'])
    env = Variable.get('ENVIRONMENT')
    return run_exports(start_time, end_time, env)


def run_validate(*args, **kwargs):
    start_time, end_time = get_start_end_time(kwargs['execution_date'])
    return validate_exports(start_time, end_time)


run_export = PythonOperator(
    task_id='run_export',
    python_callable=run_export,
    provide_context=True,
    dag=dag)

run_validate = PythonOperator(
    task_id='validate_exports',
    python_callable=run_validate,
    provide_context=True,
    dag=dag)

run_export >> run_validate
