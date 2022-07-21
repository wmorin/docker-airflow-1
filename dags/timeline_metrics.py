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
from datetime import datetime, timedelta
from utils.email_helper import email_notify
from tools.analysis.conversation_timeline.messages import TimelineMessages
from tools.analysis.conversation_timeline.timeline_metrics_etl import unify_and_load_timeline_events
from tools.analysis.conversation_timeline.conversations import TimelineConversations
from tools.utils.db_util import connect_stats_db

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
          # run every day at 3:15am PST after conversation closure
          schedule_interval='15 03 * * 1-7')


dag.doc_md = __doc__

MessagesETL = TimelineMessages()
messages_output_file = os.path.join(os.getcwd(), 'analytics_msgs.txt')
TimelineMessages.set_output_file(messages_output_file)


conversationsETL = TimelineConversations()
conversation_events_output_file = os.path.join(os.getcwd(), 'conversation_events.txt')
TimelineConversations.set_output_file(conversation_events_output_file)


stats_conn = connect_stats_db()
cur = stats_conn.cursor()
params = {
    'cursor': cur
}


def extract_conversation_events(*args, **kwargs):
    start_time = kwargs['execution_date'].subtract(days=1)
    end_time = kwargs['execution_date']
    conversationsETL.extract_conversations(start_time, end_time)


def load_conversation_events(*args, **kwargs):
    cursor = kwargs['cursor']
    conversationsETL.load_conversation_events(cursor, conversationsETL.transform_conversation_events())
    conversationsETL.close_source_db_connection()
    stats_conn.commit()


def extract_messages(*args, **kwargs):
    MessagesETL.extract_messages()


def load_messages(*args, **kwargs):
    cursor = kwargs['cursor']
    MessagesETL.load_messages(cursor, MessagesETL.transform_messages())
    MessagesETL.close_source_db_connection()


def load_unified_timeline_metrics(*args, **kwargs):
    cursor = kwargs['cursor']
    unify_and_load_timeline_events(cursor)


def close_stats_conn(*args, **kwargs):
    stats_conn.commit()
    stats_conn.close()


extract_closed_convos_from_core = PythonOperator(
    task_id='extract_closed_convos_from_core',
    python_callable=extract_conversation_events,
    provide_context=True,
    dag=dag)


load_closure_events_to_stats = PythonOperator(
    task_id='load_closure_events_to_stats',
    python_callable=load_conversation_events,
    op_kwargs=params,
    provide_context=True,
    dag=dag)


extract_closed_convo_msgs_analytics = PythonOperator(
    task_id='extract_closed_convo_msgs_analytics',
    python_callable=extract_messages,
    provide_context=True,
    dag=dag)


load_messages_to_stats = PythonOperator(
    task_id='load_messages_to_stats',
    python_callable=load_messages,
    provide_context=True,
    op_kwargs=params,
    dag=dag)


load_unified_timeline_metrics = PythonOperator(
    task_id='load_unified_timeline_metrics',
    python_callable=load_unified_timeline_metrics,
    op_kwargs=params,
    provide_context=True,
    dag=dag)


close_stats_conn = PythonOperator(
    task_id='close_stats_conn',
    python_callable=close_stats_conn,
    provide_context=True,
    dag=dag)


(extract_closed_convos_from_core >> load_closure_events_to_stats
    >> extract_closed_convo_msgs_analytics >> load_messages_to_stats
    >> load_unified_timeline_metrics
    >> close_stats_conn)
