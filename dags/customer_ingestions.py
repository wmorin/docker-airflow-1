
"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 6),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

dag = DAG('Customer_Ingestion',
          default_args=default_args,
          schedule_interval=timedelta(days=1))


"""
 TODO: simple pipeline
 - t1: Sync a folder with ingested files
 - t2: Sync a folder with customers files
 - t3: Find files that does not exists
 - t4: Run Ingestion with new files
"""


# TODO: t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='sync_ingested_files',
    bash_command='echo "sync ingested files"',
    retries=3,
    dag=dag)

t2 = BashOperator(
    task_id='sync_customer_files',
    bash_command='echo "sync customers files"',
    retries=3,
    dag=dag)

t3 = BashOperator(
    task_id='extract_different_files',
    bash_command='echo "extract different files"',
    retries=3,
    dag=dag)

t4 = BashOperator(
    task_id='run_ingestions',
    bash_command='echo "Run Ingestions"',
    dag=dag)

[t1, t2] >> t3 >> t4
