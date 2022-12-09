"""
# Collab  Metrics
This dag is to collect data from analytics into a stats db table to 
collect and organize collab events.

## Source

* Database: Anayltics,
* Tables: messages


## Target

* Database: Stats
* Table: collab_events

"""
import os
from airflow import DAG, task
from datetime import datetime, timedelta
from utils.email_helper import email_notify
from tools.analysis.collab_metrics.collab_metrics import CollabMetrics
from tools.utils.db_util import connect_stats_db


default_args = {
    'owner': 'Spencer',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 6),
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}


dag = DAG('collab_events',
          catchup=False,
          default_args=default_args,
          # run every day at 3:15am PST after conversation closure
          schedule_interval='15 03 * * 1-7')


dag.doc_md = __doc__

CollabETL = CollabMetrics()
collab_output_file = os.path.join(os.getcwd(), 'collab_events.txt')
CollabETL.set_output_file(collab_output_file)

stats_conn = connect_stats_db()
cur = stats_conn.cursor()
params = {
    'cursor': cur
}


@task(
    task_id="extract_collab_events",
    templates_dict={
        "logical_date": "{{ ds }}",
        "day_before": "{{ macros.ds_add(ds, -1) }}"
    }
)
def extract_collab_events(*args, **kwargs):
    start_time = kwargs['templates_dict']['day_before']
    end_time = kwargs['templates_dict']['logical_date']
    CollabETL.extract_messages(start_time, end_time)


@task(
    task_id="load_collab_events",
    templates_dict={
        "logical_date": "{{ ds }}",
        "day_before": "{{ macros.ds_add(ds, -1) }}",
        "cursor": cur
    }
)
def load_collab_events(*args, **kwargs):
    cursor = kwargs['templates_dict']['cursor']
    CollabETL.load_messages(cursor, CollabETL.transform_message())
    CollabETL.close_source_db_connection()
    stats_conn.commit()


@task(
    task_id="close_stats_conn"
)
def close_stats_conn(*args, **kwargs):
    stats_conn.commit()
    stats_conn.close()


extract_collab_events >> load_collab_events >> close_stats_conn