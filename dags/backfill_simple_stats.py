"""
# Backfill Conversation Stats
This dag is to backfill agent's analytics data from agent's interaction with dashboard.

## Source
* Database: Anayltics
* Tables: messages

* Database: Core
* Tables: teams, agents, categories, tags

## Return
* Database: Stats,
* Tables: conversations

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
    'start_date': datetime(2020, 5, 27),
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}

dag = DAG('backfill_simple_stats_for_conversation',
          catchup=False,
          default_args=default_args)
dag.doc_md = __doc__

# It is not recommanded to use Variable with global scope
# but not sure if there is another way to inject airflow variables
# into envionment variables.
env = os.environ.copy()
env.update(get_environments())

backfill_simple_stats = BashOperator(
    task_id='simple_stats_backfill_script',
    bash_command="""bash python-tools/scripts/backfill_simple_stats.sh 
                 {{ dag_run.conf['start_date'] }} {{ dag_run.conf['end_date'] }}
                 """,
    retries=1,
    schedule_interval=None,
    env=env,
    dag=dag)
