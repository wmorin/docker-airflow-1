"""
# Data Retention (Conversation Stats)
This dag is to expunge data from core db and s3 buckets after a fixed time period

## Source
* Database: Core DB,
* Tables: messages

"""

import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from utils.airflow_helper import get_environments
from utils.email_helper import email_notify

default_args = {
    'owner': 'Akshay',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 12),
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}
env = os.environ.copy()
env.update(get_environments())

params = {
    'bucket': f"agentiq-{env['ENVIRONMENT']}-assets",
    'duration_value':  env['DATA_RETENTION_DURATION'],
}


dag = DAG('daily_core_s3_data_retention',
          catchup=False,
          default_args=default_args,
          # run every day at 3:30am PST after conversation closure
          schedule_interval='30 04 * * 1-7',
          params=params)
dag.doc_md = __doc__

# It is not recommended to use Variable with global scope
# but not sure if there is another way to inject airflow variables
# into environment variables.

daily_core_s3_data_retention = BashOperator(
    task_id='data_retention_script',
    bash_command='python3 -m tools.data_retention \
            --duration="{{ params.duration_value }}" \
            --bucket_name="{{ params.bucket }}"',
    retries=1,
    env=env,
    dag=dag)
