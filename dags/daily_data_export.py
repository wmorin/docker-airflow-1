import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.airflow_helper import get_environments
from api_exports.run_exports import run_exports


default_args = {
    'owner': 'Jaekwan',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 31),
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}

dag = DAG('daily_data_export',
          default_args=default_args,
          # run every day at 3:10am PST after conversation closure
          schedule_interval='10 3 * * 1-7')


def run_export(*args, **kwargs):
    # time might need to fine control again
    start_time = kwargs['execution_date'].subtract(days=1).format("%Y-%m-%d %H:%M:%S")
    end_time = kwargs['execution_date'].format("%Y-%m-%d %H:%M:%S")

    env = Variable.get('ENVIRONMENT')
    # set environment variables
    os.environ.update(get_environments())

    return run_exports(start_time, end_time, env)


run_export = PythonOperator(
    task_id='run_export',
    python_callable=run_export,
    provide_context=True,
    dag=dag)
