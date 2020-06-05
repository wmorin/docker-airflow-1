"""
To run locally through script,
Use bash script:
    bash ./python-tool/script/backfill_customer_events.sh 2020-06-01 2020-06-10
"""
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from utils.aws_helper import make_s3_key
from utils.airflow_helper import get_environments


default_args = {
    'owner': 'Jaekwan',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 1),
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

# It is not recommanded to use Variable with global scope
# but not sure if there is another way to inject airflow variables
# into envionment variables.
env = os.environ.copy()
env.update(get_environments())
env.update({'TIMEZONE': Variable.get('TIMEZONE')})

CUSTOMER_EVENT_PATH = 'customer_events'

params = {
    'bucket': Variable.get('ETL_S3_BUCKET'),
    'bucket_key': make_s3_key(env['ENVIRONMENT'], CUSTOMER_EVENT_PATH),
    'shared_dir':  '/tmp/customer_events_files',
    'temp_file_path': '/tmp/customer_events_files/tmp/' + env['ENVIRONMENT'],
    'diff_dir_path': '/tmp/ingestion_diff_dir'}

dag = DAG('daily_customer_event_metrics',
          default_args=default_args,
          # run every day at 3:40am PST after conversation closure
          schedule_interval='40 10 * * 1-7',
          params=params)

# local folder paths = /tmp/customer_events_files/tmp/{env}/%Y-%m-%d
DESTINATION_PATH = '{{ params.temp_file_path }}/{{ execution_date.subtract(days=1).format("%Y-%m-%d") }}'

# First collect the events data from all the resources and write to files
collection_customer_events = BashOperator(
    task_id='collection_customer_events',
    bash_command='python3 -m tools.analysis.customer_events_metrics'
                 + ' --start_date="{{ execution_date.subtract(days=1).format("%Y-%m-%d") }} 00:00:00"'
                 + ' --end_date="{{ execution_date.subtract(days=1).format("%Y-%m-%d") }} 23:59:59"'
                 + ' --populate_customer_events'
                 + ' --timezone="{{ var.value.TIMEZONE }}"'
                 + ' --output_dir="{{ params.shared_dir }}"',
    retries=1,
    env=env,
    dag=dag)


# Second, join the data and verify consistency
join_data = BashOperator(
    task_id='join_the_data_and_verify_consistency',
    bash_command='python3 -m tools.analysis.customer_events_join'
            + f' --stats_data_filename={DESTINATION_PATH}/stats_events.csv'
            + f' --core_db_data_filename={DESTINATION_PATH}/core_db_events.csv',
    retries=1,
    env=env,
    dag=dag)

# Third, upload the data to the DB.
# TODO: FULL_OUTPUT_DIR
upload_to_db = BashOperator(
    task_id='upload_to_db',
    bash_command='python3 -m tools.analysis.customer_events_metrics'
                 + ' --upload_to_db'
                 + ' --table_name=customer_events'
                 + f' --filename_to_load={DESTINATION_PATH}/stats_customer_events.csv',
    retries=1,
    env=env,
    dag=dag)

# Fourth, upload data to active users from customer events
upload_active_user_to_db = BashOperator(
    task_id='upload_active_user_to_db',
    bash_command='python3 -m tools.analysis.customer_events_metrics'
                 + ' --end_date="{{ execution_date.subtract(days=1).format("%Y-%m-%d") }} 23:59:59"'
                 + ' --populate_active_customers',
    retries=1,
    env=env,
    dag=dag)

S3_KEY_DIR = 's3://{{params.bucket}}/{{params.bucket_key}}/{{ execution_date.subtract(days=1).format("%Y-%m-%d") }}'
# Update files to external storage
upload_result_to_s3 = BashOperator(
    task_id='upload_csv_files_to_s3',
    bash_command=f'aws s3 cp {DESTINATION_PATH} ' + f'{S3_KEY_DIR} --recursive',
    retries=3,
    dag=dag)

collection_customer_events >> join_data >> upload_to_db >> upload_active_user_to_db >> upload_result_to_s3
