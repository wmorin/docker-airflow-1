"""
# Customer Selections
This dag is to process agent's analytics data from customer's interaction with dialogs.

## Source
* Database: Anayltics,
* Tables: messages

## Return
* Database: Stats,
* Tables: customer_selections, questions

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
    'start_date': datetime(2021, 5, 21),
    'email': ['swe@agentiq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}

dag = DAG('daily_customer_selections',
          catchup=False,
          default_args=default_args,
          # run every day at 4:30am PST after conversation closure
          schedule_interval='30 04 * * 1-7')
dag.doc_md = __doc__

# It is not recommanded to use Variable with global scope
# but not sure if there is another way to inject airflow variables
# into envionment variables.
env = os.environ.copy()
env.update(get_environments())

daily_customer_selections = BashOperator(
    task_id='customer_selections_script',
    bash_command='python -m tools.analysis.customer_selections.populate_customer_selections \
            --start_date="{{ ds }} 00:00:00" \
            --end_date="{{ ds }} 23:59:59" \
            --timezone="{{ var.value.TIMEZONE }}"',
    retries=1,
    env=env,
    dag=dag)
