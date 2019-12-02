
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

from utils.analytics_helper import get_daily_weekly_script

tag_name = '036978135238.dkr.ecr.us-east-1.amazonaws.com/agentiq/ai-engine:s1-latest'

params = {
    'tag_name': Variable.get('DOCKER_IMAGE_AI_ENGINE') or tag_name}


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 27),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

dag = DAG('Daily_Analytics',
          default_args=default_args,
          schedule_interval='00 14 30 * * 1-7',
          params=params)


t1 = BashOperator(
    task_id='docker_login',
    bash_command='$(aws ecr get-login --no-include-email --region us-east-1)',
    retries=3,
    dag=dag)


t2 = BashOperator(
    task_id='fetch_ai_engine_docker_file',
    bash_command='docker pull {{params.tag_name}}',
    retries=3,
    dag=dag)

env = {
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
        'ANALYTICS_DB_PASSWORD': Variable.get('ANALYTICS_DB_PASSWORD')}

# TODO(jaekwan): s3 credential is not binding with volume. Therefore, uploading to s3 fails at the moment.
t3 = BashOperator(
    task_id='run_daily_simple_stats',
    bash_command='docker run -v ~/.aws:/root/.aws {{params.tag_name}} ' + get_daily_weekly_script('daily', env),
    retries=3,
    dag=dag)

t4 = BashOperator(
    task_id='Finish',
    bash_command='echo "Daily Analytics Done"',
    retries=1,
    dag=dag)

t1 >> t2 >> t3 >> t4
