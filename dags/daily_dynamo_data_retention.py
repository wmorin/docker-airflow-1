"""
# Data Retention (Dynamo Conversations)
This dag is to expunge chat data from dynamo after a fixed time period

## Source
* Database: dynamo,
* Tables: conversations

"""

import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.airflow_helper import get_environments
from utils.email_helper import email_notify
from aiqdynamo.tables.conversations import ConversationsTable
from tools.utils.time_util import get_n_months_from_now_string

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




dag = DAG('daily_dynamo_data_retention',
          catchup=False,
          default_args=default_args,
          # run every day at 5:30am PST after conversation closure
          schedule_interval='30 05 * * 1-7'
          )
dag.doc_md = __doc__

def empty_chat_transcripts(*args, **kwargs):
    duration = env['DATA_RETENTION_DURATION_MONTHS']
    past_timestamp = get_n_months_from_now_string((-1) * duration)
    items = ConversationsTable.get_conversation_records(end_date=past_timestamp)
    for item in items:
        ConversationsTable.set_chat_transcript_empty(item)


# It is not recommended to use Variable with global scope
# but not sure if there is another way to inject airflow variables
# into environment variables.

run_dynamo_retention_task = PythonOperator(
    task_id='empty_dynamo_chats',
    python_callable=empty_chat_transcripts,
    provide_context=True,
    dag=dag)

