
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 19),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

dag = DAG('Test_Run_docker_in_docker',
          default_args=default_args,
          schedule_interval=timedelta(days=1))

t1 = BashOperator(
    task_id='docker_login',
    bash_command='$(aws ecr get-login --no-include-email --region us-east-1)',
    retries=3,
    dag=dag)

tag_name = '036978135238.dkr.ecr.us-east-1.amazonaws.com/agentiq/base-api:s1-latest'

t2 = BashOperator(
    task_id='fetch_docker_file',
    bash_command=f'docker pull {tag_name}',
    retries=3,
    dag=dag)

t1 >> t2
