"""
# Timeline  Metrics
This dag is to collect data from analytics, core db into a singular stats db table
to facilitate viewing timeline data for each conversation.

## Source
The following events are collected from analytics database

* Database: Anayltics,
* Tables: messages
The following events are collected from core database

* Database: Core,
* Tables: agents_logs


## Target

* Database: Stats
* Tables: timeline_events

"""
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from utils.airflow_helper import get_connection
from logger import logger
from utils.aws_helper import make_s3_key
from tools.utils.aws_util import s3_upload_file, s3_download_file
from tools.utils.file_util import dump_to_csv_file, load_csv_file
from utils.email_helper import email_notify
from tools.analysis.conversation_timeline.messages import TimelineMessages


default_args = {
    'owner': 'Akshay',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 21),
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}


dag = DAG('timeline_events',
          catchup=False,
          default_args=default_args,
          schedule_interval=timedelta(days=1))
dag.doc_md = __doc__

MessagesETL = TimelineMessages()
messages_output_file = os.path.join(os.getcwd(), 'analytics_msgs.txt')

def extract_messages(*args, **kwargs):
    start_time = kwargs['execution_date'].subtract(days = 1)
    end_time = kwargs['execution_date']
    MessagesETL.set_output_file(messages_output_file)
    MessagesETL.extract_messages(start_time,
                                 end_time)


def load_messages(*args, **kwargs):
    load_data(messages_output_file,
              MessagesETL.transform_message)


extract_messages = PythonOperator(
    task_id='extract_messages',
    python_callable=extract_messages,
    provide_context=True,
    dag=dag)


load_messages = PythonOperator(
    task_id='load_messages',
    python_callable=load_messages,
    provide_context=True,
    dag=dag)

extract_messages >> load_messages
