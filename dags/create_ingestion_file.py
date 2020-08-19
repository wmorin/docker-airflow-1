from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}

params = {
    'empty_file_path': '/tmp/ingestion_empty_files',
    's3_ingestion_file_location': Variable.get('S3_INGESTION_FILE_LOCATION')}


dag = DAG('create_ingestion_file',
          default_args=default_args,
          schedule_interval=None,   # Do not trigger periodically
          params=params)

t1 = BashOperator(
    task_id='create_empty_file_locally',
    bash_command='touch {{params.empty_file_path}}',
    retries=3,
    dag=dag)

t2 = BashOperator(
    task_id='copy_empty_file_remotely',
    bash_command='aws s3 cp {{params.empty_file_path}} {{params.s3_ingestion_file_location}}',
    retries=3,
    dag=dag)

t1 >> t2
