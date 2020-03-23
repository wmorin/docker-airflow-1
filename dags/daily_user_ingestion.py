import shutil
import requests

from os import listdir, makedirs
from os.path import exists, join
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 5),
    'email': ['software_engineering@agentiq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

params = {
    'src_file_path': '/tmp/ingestion_src_files',
    's3_file_path': '/tmp/ingestion_target_files',
    'diff_file_path': '/tmp/ingestion_diff_files',
    'diff_dir_path': '/tmp/ingestion_diff_dir',
    'SFTP_HOST': Variable.get('SFTP_HOST'),
    'SFTP_PORT': Variable.get('SFTP_PORT'),
    'SFTP_USER': Variable.get('SFTP_USER'),
    'SFTP_PASSWORD': Variable.get('SFTP_PASSWORD'),
    's3_ingestion_file_location': Variable.get('S3_INGESTION_FILE_LOCATION'),
    'base_api_host': Variable.get('BASE_API_INTERNAL_HOST'),
    'jwt_token': Variable.get('JWT_TOKEN')}


dag = DAG('daily_user_ingestion',
          catchup=False,
          default_args=default_args,
          schedule_interval='30 7 * * *',
          params=params)


def be_ready(*args, **kwargs):
    """ Clean up temporary working directories """
    dst_local_path = kwargs['diff_dir_path']

    # if exist, clean them up and create
    for path in [dst_local_path]:
        if exists(path):
            shutil.rmtree(path)
        makedirs(path)
    return str([dst_local_path])


def ingest_files(*args, **kwargs):
    dst_local_path = kwargs['diff_dir_path']
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

        headers = {'Authorization': f'Bearer {jwt}'}

        # Trigger ingestion endpoint
        files = {'file': (name, open(full_path, 'rb'), 'text/csv')}

        resource = url + path
        print(f'sending: {resource}, {full_path}')
        res = requests.post(resource, files=files, headers=headers)

        if res.status_code != 200:
            print(f'Error: {res}')
            raise Exception(f'Unable to ingest file: {res}')
        done.append(name)

    return str(done)


t0 = PythonOperator(
    task_id='ready_for_ingestion',
    python_callable=be_ready,
    op_kwargs=params,
    dag=dag)

# Get sftp server list
cmd1 = 'eval sync_sftp {{params.SFTP_HOST}} {{params.SFTP_PORT}} {{params.SFTP_USER}} {{params.SFTP_PASSWORD}} list' \
    '| grep -v sftp' \
    '| grep .csv' \
    '| sort -i' \
    '> {{params.src_file_path}}' \
    '&& cat {{params.src_file_path}}'
t1 = BashOperator(
    task_id='get_server_file_list',
    bash_command=cmd1,
    retries=3,
    dag=dag)

# Get ingested list
cmd2 = 'aws s3 cp {{params.s3_ingestion_file_location}} {{params.s3_file_path}}' \
    '&& cat {{params.s3_file_path}}'
t2 = BashOperator(
    task_id='get_ingested_file_list',
    bash_command=cmd2,
    retries=3,
    dag=dag)

# Get diff list
cmd3 = 'comm -23 {{params.src_file_path}} {{params.s3_file_path}} > {{params.diff_file_path}}' \
    '&& cat {{params.diff_file_path}}'
t3 = BashOperator(
    task_id='get_difference',
    bash_command=cmd3,
    retries=3,
    dag=dag)

# Download diff list
cmd4 = 'cd {{params.diff_dir_path}}' \
    '&& cat {{params.diff_file_path}}' \
    '| tr "\r\n" " " ' \
    '| xargs -I {} bash -c "eval ' \
    'sync_sftp {{params.SFTP_HOST}} {{params.SFTP_PORT}} {{params.SFTP_USER}} {{params.SFTP_PASSWORD}} fetch {}"' \
    '&& cd -'
t4 = BashOperator(
    task_id='download_diff_files_only',
    bash_command=cmd4,
    retries=3,
    dag=dag)

# Ingest files
t5 = PythonOperator(
    task_id='ingest_diff_files',
    python_callable=ingest_files,
    op_kwargs=params,
    dag=dag)

# Update ingested files
t6 = BashOperator(
    task_id='upload_ingested_file_list',
    bash_command='aws s3 cp {{params.src_file_path}} {{params.s3_ingestion_file_location}}',
    retries=3,
    dag=dag)

t0 >> [t1, t2] >> t3 >> t4 >> t5 >> t6
