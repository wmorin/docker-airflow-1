import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable


default_args = {
    'owner': 'Jaekwan',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 27),
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

dag = DAG('Daily_simple_stats_for_conversation',
          default_args=default_args,
          # run every day at 3:30am PST after conversation closure
          schedule_interval='30 10 * * 1-7')

# It is not recommanded to use Variable with global scope
# but not sure if there is another way to inject airflow variables
# into envionment variables.
env = os.environ.copy()
env.update({
        'ENVIRONMENT': Variable.get('ENVIRONMENT'),
        'BASE_API_DB_PORT':  Variable.get('BASE_API_DB_PORT'),
        'BASE_API_DB_HOST': Variable.get('BASE_API_DB_HOST'),
        'BASE_API_DB_NAME': Variable.get('BASE_API_DB_NAME'),
        'BASE_API_DB_USERNAME': Variable.get('BASE_API_DB_USERNAME'),
        'BASE_API_DB_PASSWORD': Variable.get('BASE_API_DB_PASSWORD'),
        'ANALYTICS_DB_PORT': Variable.get('ANALYTICS_DB_PORT'),
        'ANALYTICS_DB_HOST': Variable.get('ANALYTICS_DB_HOST'),
        'ANALYTICS_DB_NAME': Variable.get('ANALYTICS_DB_NAME'),
        'ANALYTICS_DB_USERNAME': Variable.get('ANALYTICS_DB_USERNAME'),
        'ANALYTICS_DB_PASSWORD': Variable.get('ANALYTICS_DB_PASSWORD'),
        'STATS_DB_PORT': Variable.get('STATS_DB_PORT'),
        'STATS_DB_HOST': Variable.get('STATS_DB_HOST'),
        'STATS_DB_NAME': Variable.get('STATS_DB_NAME'),
        'STATS_DB_USERNAME': Variable.get('STATS_DB_USERNAME'),
        'STATS_DB_PASSWORD': Variable.get('STATS_DB_PASSWORD')})

daily_simple_stats = BashOperator(
    task_id='simple_stats_script',
    bash_command='python -m tools.analysis.simple_stats \
            --start_date="{{ execution_date.subtract(days=1).format("%Y-%m-%d %H:%M:%S") }}" \
            --end_date="{{ execution_date.format("%Y-%m-%d %H:%M:%S") }}" \
            --message_env_filter={{ var.value.ENVIRONMENT }} \
            --expand_to_full_conversations \
            --store_convo_stats',
    retries=1,
    env=env,
    dag=dag)
