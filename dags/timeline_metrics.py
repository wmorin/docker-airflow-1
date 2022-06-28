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
from tools.analysis.conversation_timeline.timeline_metrics_etl import push_to_timeline_table
from tools.analysis.conversation_timeline.agent_events import TimelineAgents
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
          # run every day at 1:50am PST
          schedule_interval='50 01 * * 1-7')


dag.doc_md = __doc__

MessagesETL = TimelineMessages()
messages_output_file = os.path.join(os.getcwd(), 'analytics_msgs.txt')
TimelineMessages.set_output_file(messages_output_file)

AgentsETL = TimelineAgents()
agent_events_output_file = os.path.join(os.getcwd(), 'agent_events.txt')
TimelineAgents.set_output_file(agent_events_output_file)


convoETL = TimelineConversations()
conversation_events_output_file = os.path.join(os.getcwd(), 'conversation_events.txt')
TimelineConversations.set_output_file(conversation_events_output_file)


stats_conn = connect_stats_db()
cur = stats_conn.cursor()
params = {
    'cursor': cur
}


def extract_messages(*args, **kwargs):
    start_time = kwargs['execution_date'].subtract(days=1)
    end_time = kwargs['execution_date']
    MessagesETL.extract_messages(start_time, end_time)


def load_messages(*args, **kwargs):
    cursor = kwargs['cursor']
    push_to_timeline_table(MessagesETL.transform_messages(), cursor)


def extract_agent_events(*args, **kwargs):
    start_time = kwargs['execution_date'].subtract(days=1)
    end_time = kwargs['execution_date']
    AgentsETL.extract_agent_events(start_time, end_time)


def load_agent_events(*args, **kwargs):
    cursor = kwargs['cursor']
    push_to_timeline_table(AgentsETL.transform_agent_events(), cursor)


def extract_conversation_events(*args, **kwargs):
    start_time = kwargs['execution_date'].subtract(days=1)
    end_time = kwargs['execution_date']
    convoETL.extract_conversations(start_time, end_time)


def load_conversation_events(*args, **kwargs):
    cursor = kwargs['cursor']
    push_to_timeline_table(convoETL.transform_conversation_events(), cursor)


def close_connections(*args, **kwargs):
    MessagesETL.close()
    AgentsETL.close()
    convoETL.close()
    stats_conn.close()


extract_messages = PythonOperator(
    task_id='extract_messages',
    python_callable=extract_messages,
    provide_context=True,
    dag=dag)


load_messages = PythonOperator(
    task_id='load_messages',
    python_callable=load_messages,
    provide_context=True,
    op_kwargs=params,
    dag=dag)


extract_agent_events = PythonOperator(
    task_id='extract_agent_events',
    python_callable=extract_agent_events,
    provide_context=True,
    dag=dag)


load_agent_events = PythonOperator(
    task_id='load_agent_events',
    python_callable=load_agent_events,
    op_kwargs=params,
    provide_context=True,
    dag=dag)


extract_conversation_events = PythonOperator(
    task_id='extract_agent_events',
    python_callable=extract_conversation_events,
    provide_context=True,
    dag=dag)


load_conversation_events = PythonOperator(
    task_id='load_agent_events',
    python_callable=load_conversation_events,
    op_kwargs=params,
    provide_context=True,
    dag=dag)


close_connections = PythonOperator(
    task_id='close_connections',
    python_callable=close_connections,
    provide_context=True,
    dag=dag)


(extract_messages >> load_messages
 >> extract_agent_events >> load_agent_events
 >> extract_conversation_events >> load_conversation_events
 >> close_connections)
