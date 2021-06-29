"""
# Agent Event Metrics
This dag is to process agent's analytics data from agent's interaction with dashboard.

The visible UI for this data is under Metrics > Coach > Document/Asset/Suggestion in dashboard UI.

## Source
The following events are collected from analytics database

* Database: Anayltics,
* Tables: documents, assets, suggestions

## Intermediary Storage (S3)
The tasks first retrieves data from anaytics and copy to aws s3 with the below path
([Go](https://console.aws.amazon.com/s3/buckets/agentiq-etl/?region=us-east-1&tab=overview))
> s3://agentiq-etl/{env}/agent_event/daily/{date}/agent_(suggestion|document|asset)_events.csv

## Return
The second script fetches data from s3 and filters only interested data. Finally, it stores them into agent events.

* Database: Stats
* Tables: agent_events

"""
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta

from logger import logger
from utils.aws_helper import make_s3_key
from tools.utils.aws_util import s3_upload_file, s3_download_file
from tools.utils.file_util import dump_to_csv_file, load_csv_file
from utils.email_helper import email_notify

default_args = {
    'owner': 'Jaekwan',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 27),
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}

dag = DAG('agent_event_metrics',
          catchup=False,
          default_args=default_args,
          schedule_interval=timedelta(days=1))
dag.doc_md = __doc__

AGENT_EVENT_PATH = 'agent_event'


def get_connection(name):
    return PostgresHook(postgres_conn_id=name).get_conn()


def get_filename(name):
    return f'agent_{name}_events.csv'


def analytics_select_query(table, interested_events, start_time, end_time):
    return f"""SELECT payload->>'action', payload->>'conversation_id', date
        FROM {table}
        WHERE payload->>'action' in {interested_events}
        AND payload->>'conversation_id' is NOT NULL
        AND date between '{start_time}'::timestamp and '{end_time}'::timestamp;"""


def stats_upsert_query(name, conv_id, stamp):
    table = 'agent_events'
    return f"""INSERT INTO {table} (event_name, conversation_id, time_stamp)
        VALUES ('{name}', {conv_id}, '{stamp}'::timestamp)
        ON CONFLICT ON CONSTRAINT uniq_event_per_conversation
        DO UPDATE
        SET event_name='{name}', conversation_id={conv_id}, time_stamp='{stamp}'
        WHERE {table}.event_name='{name}' AND {table}.conversation_id={conv_id} AND {table}.time_stamp='{stamp}';"""


def move_analytics_data_into_s3(analytics_query, name, execution_date):
    analytics_conn = get_connection('ANALYTICS_DB')

    if (not analytics_conn):
        raise Exception('Unable to connect to analytics database')

    analytics_cursor = analytics_conn.cursor()
    analytics_cursor.execute(analytics_query)
    rows = analytics_cursor.fetchall()
    analytics_conn.close()

    logger.info(f'Total: {len(rows)} records')

    file_name = f'agent_{name}_events.csv'
    dump_to_csv_file(os.path.join(os.getcwd(), file_name),
                     ['event_name', 'conversation_id', 'time_stamp'],
                     rows)

    bucket = Variable.get('ETL_S3_BUCKET')
    env = Variable.get('ENVIRONMENT')
    key = make_s3_key(env,
                      AGENT_EVENT_PATH,
                      date_string=execution_date.to_date_string())
    s3_upload_file(bucket, file_name, key)
    logger.info(f'File uploaded: {file_name} {key} {bucket}')
    return f's3://{bucket}/{key}/{file_name}'


def move_s3_data_into_stats(name, execution_date):
    """ Copy Csv from s3 and move to stats db """
    logger.info(f'move_s3_data_into_stats: {name} {execution_date}')

    # Download from s3
    file_name = get_filename(name)
    bucket = Variable.get('ETL_S3_BUCKET')
    env = Variable.get('ENVIRONMENT')
    s3_file_path = make_s3_key(env,
                               AGENT_EVENT_PATH,
                               date_string=execution_date.to_date_string(),
                               file_name=file_name)
    downloaded = os.path.join(os.getcwd(), file_name)
    s3_download_file(bucket, s3_file_path, downloaded)
    logger.info(f'File download: {file_name} {s3_file_path} {bucket}')

    # Load CSV
    headers, rows = load_csv_file(downloaded)

    # Upsert to Stats
    stats_conn = get_connection('STATS_DB')
    if not stats_conn:
        raise Exception('Unable to connect to stats database')

    stats_cursor = stats_conn.cursor()
    if not rows:
        return 0

    logger.info(f'Upserting # {len(rows)}')
    for row in rows:
        query = stats_upsert_query(row[0], row[1], row[2])
        stats_cursor.execute(query)
        stats_conn.commit()

    stats_cursor.close()
    return len(rows)


def get_config(name):
    conf = {
        'suggestion': {
            'analytics_table': 'suggestions',
            'events': ('conversations.suggest.send',
                       'conversations.suggest.click',
                       'conversations.suggest.edit',
                       'conversations.suggest.show')},
        'asset': {
            'analytics_table': 'assets',
            'events': ('conversations.kb.assets.send',
                       'conversations.kb.assets.click',
                       'conversations.kb.assets.show')},
        'document': {
            'analytics_table': 'documents',
            'events': ('conversations.kb.documents.send',
                       'conversations.kb.documents.click',
                       'conversations.kb.documents.show')}}

    if name not in conf:
        raise Exception(f'Unable to find configuration for {name}')

    return conf[name]


def move_analytics_to_s3(*args, **kwargs):
    name = kwargs['agent_event_name']
    start_time = kwargs['execution_date'].subtract(days=1)
    end_time = kwargs['execution_date']
    conf = get_config(name)

    return move_analytics_data_into_s3(analytics_select_query(conf['analytics_table'],
                                                              conf['events'],
                                                              start_time,
                                                              end_time),
                                       name,
                                       end_time)


def move_suggestion_to_s3(*args, **kwargs):
    kwargs['agent_event_name'] = 'suggestion'
    return move_analytics_to_s3(*args, **kwargs)


def move_suggestion_s3_to_stats(*args, **kwargs):
    return move_s3_data_into_stats('suggestion', kwargs['execution_date'])


def move_asset_to_s3(*args, **kwargs):
    kwargs['agent_event_name'] = 'asset'
    return move_analytics_to_s3(*args, **kwargs)


def move_asset_s3_to_stats(*args, **kwargs):
    return move_s3_data_into_stats('asset', kwargs['execution_date'])


def move_document_to_s3(*args, **kwargs):
    kwargs['agent_event_name'] = 'document'
    return move_analytics_to_s3(*args, **kwargs)


def move_document_s3_to_stats(*args, **kwargs):
    return move_s3_data_into_stats('document', kwargs['execution_date'])


suggestion_to_s3 = PythonOperator(
    task_id='suggestion_event_to_s3',
    python_callable=move_suggestion_to_s3,
    provide_context=True,
    dag=dag)

suggestion_s3_to_stats = PythonOperator(
    task_id='s3_suggestion_event_to_stats',
    python_callable=move_suggestion_s3_to_stats,
    provide_context=True,
    dag=dag)

asset_to_s3 = PythonOperator(
    task_id='asset_event_to_s3',
    python_callable=move_asset_to_s3,
    provide_context=True,
    dag=dag)

asset_s3_to_stats = PythonOperator(
    task_id='s3_asset_event_to_stats',
    python_callable=move_asset_s3_to_stats,
    provide_context=True,
    dag=dag)

document_to_s3 = PythonOperator(
    task_id='document_event_to_s3',
    python_callable=move_document_to_s3,
    provide_context=True,
    dag=dag)

document_s3_to_stats = PythonOperator(
    task_id='s3_document_event_to_stats',
    python_callable=move_document_s3_to_stats,
    provide_context=True,
    dag=dag)


suggestion_to_s3 >> suggestion_s3_to_stats
asset_to_s3 >> asset_s3_to_stats
document_to_s3 >> document_s3_to_stats
