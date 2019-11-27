
"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from os import listdir, makedirs
from os.path import isfile, exists, join
import shutil
import random
import string
import requests

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

params = {
    'src_file_path': '/tmp/ingestion_src_files',
    's3_file_path': '/tmp/ingestion_target_files',
    'diff_file_path': '/tmp/ingestion_diff_files',
    'HOST': 'transfer.arvest.com',
    'PORT': '9992',
    'USER': 'arvest-ops',
    'PSSWD': 'Ay4qkMCJbKUdsw8G',
    's3_location': 's3://agentiq-dev-sandbox/customers',
    'tag_name': '036978135238.dkr.ecr.us-east-1.amazonaws.com/agentiq/base-api:s1-latest',
    'base_api_host': 'http://192.168.0.114:8000',
    'jwt_token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImJvdEBhZ2VudGlxLmNvbSIsInJvbGUiOiJhZG1pbiIsImlhdCI6MTUyNTgwNDIxMCwiYXVkIjoiWGVacnNjUDQwTDIyek01MFhuOFY1NmRxelFzRVNQV0ciLCJpc3MiOiJodHRwczovL2FnZW50aXEuYXV0aDAuY29tLyJ9.xzMTiIsPOw1L3IWZboIlkjCH2p9cv2N_svXM3AsNCz4'}


dag = DAG('Customer_Ingestion',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          params=params)


def be_ready(*args, **kwargs):
    """ Clean up temporary working directories """
    src_local_path = kwargs['src_file_path']
    target_local_path = kwargs['s3_file_path']
    dst_local_path = kwargs['diff_file_path']

    # if exist, clean them up and create
    for path in [src_local_path, target_local_path, dst_local_path]:
        if exists(path):
            shutil.rmtree(path)
        makedirs(path)
    return str([src_local_path, target_local_path, dst_local_path])

def find_file_diffs(*args, **kwargs):
    src_local_path = kwargs['src_file_path']
    target_local_path = kwargs['s3_file_path']
    dst_local_path = kwargs['diff_file_path']

    if not (src_local_path and target_local_path and dst_local_path):
        raise Exception(f'input error: {src_local_path} {target_local_path} {dst_local_path}')

    hasFile = {k: True for k in listdir(target_local_path)}

    ret = []
    for name in listdir(src_local_path):
        if name not in hasFile:
            # copy to dst_local_path
            shutil.copyfile(join(src_local_path, name), join(dst_local_path, name))
            ret.append(name)

    return str(ret)

def ingest_files(*args, **kwargs):
    dst_local_path = kwargs['diff_file_path']
    url = kwargs['base_api_host']
    jwt = kwargs['jwt_token']
    path = '/ingestion/users'

    if not dst_local_path:
        raise Exception(f'input error: destination folder doesn\'t exist {dst_local_path}')

    print(f'files: {listdir(dst_local_path)}')
    done = []

    for name in listdir(dst_local_path):
        full_path = join(dst_local_path, name)

        if not exists(full_path):
            print(f'File does not exist: {full_path}')
            continue

        headers = { 'Authorization': f'Bearer {jwt}'}

        # Trigger ingestion endpoint
        files = {'file': (name, open(full_path, 'rb'), 'text/csv')}

        resource = url + path
        print(f'sending: {resource}, {full_path}')
        res = requests.post(resource, files=files, headers=headers)

        if res.status_code != 200:
            print(f'Error: {res}');
            raise Exception(f'Unable to ingest file: {res}')
        done.append(name)

        # TODO: update s3 on every success

    return str(done)


t0 = PythonOperator(
    task_id='ready_for_ingestion',
    python_callable=be_ready,
    op_kwargs=params,
    dag=dag)

t1 = BashOperator(
    task_id='get_server_file_list',
    bash_command='cd {{params.src_file_path}} && eval sync_sftp {{params.HOST}} {{params.PORT}} {{params.USER}} {{params.PSSWD}} fetch && cd -',
    # TODO: Get only file diff
    #bash_command=f'sync_sftp {HOST} {PORT} {USER} {PSSWD} list | grep -v sftp | grep .csv | sort > /tmp/serv_files',
    retries=3,
    dag=dag)

t2 = BashOperator(
    task_id='sync_customer_files',
    bash_command='aws s3 sync {{params.s3_location}} {{params.s3_file_path}}',
    retries=3,
    dag=dag)

t3 = PythonOperator(
    task_id='extract_only_diff_files',
    python_callable=find_file_diffs,
    op_kwargs=params,
    dag=dag)

t4 = PythonOperator(
    task_id='ingest_diff_files',
    python_callable=ingest_files,
    op_kwargs=params,
    dag=dag)


t0 >> [t1, t2] >> t3 >> t4
