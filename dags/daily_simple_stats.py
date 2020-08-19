"""
# Simple Stats (Conversation Stats)
This dag is to process agent's analytics data from agent's interaction with dashboard.

## Source
* Database: Anayltics,
* Tables: messages

## Return
* Database: Stats,
* Tables: conversations

"""

import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from utils.airflow_helper import get_environments


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

dag = DAG('Daily_simple_stats_for_conversation',
          catchup=False,
          default_args=default_args,
          # run every day at 3:30am PST after conversation closure
          schedule_interval='30 03 * * 1-7')
dag.doc_md = __doc__

# It is not recommanded to use Variable with global scope
# but not sure if there is another way to inject airflow variables
# into envionment variables.
env = os.environ.copy()
env.update(get_environments())

daily_simple_stats = BashOperator(
    task_id='simple_stats_script',
    bash_command='python -m tools.analysis.simple_stats \
            --start_date="{{ execution_date.format("%Y-%m-%d") }} 00:00:00" \
            --end_date="{{ execution_date.format("%Y-%m-%d") }} 23:59:59" \
            --timezone="{{ var.value.TIMEZONE }}" \
            --message_env_filter={{ var.value.ENVIRONMENT }} \
            --expand_to_full_conversations \
            --store_convo_stats',
    retries=1,
    env=env,
    dag=dag)
