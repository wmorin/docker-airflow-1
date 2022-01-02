from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "email": ["support@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2021, 1, 1),
}

def parsing():
    return True

def processing():
    return True

with DAG("simple_pipe", default_args=default_args, schedule_interval="*/5 * * * *", catchup=False) as dag:
    t1 = PythonOperator(task_id="parsing", python_callable=parsing)
    t2 = PythonOperator(task_id="processing", python_callable=processing)
    t3 = BashOperator(task_id="storing", bash_command="exit 0")

    t1 >> t2 >> t3
