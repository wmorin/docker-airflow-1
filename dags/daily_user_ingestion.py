import shutil
import requests
from collections import defaultdict

from os import listdir, makedirs, linesep
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
    'email': ['swe@agentiq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_notify
}

params = {
    'intermediate_file_store': '/tmp/ingestion_intermediate_src_files',
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


def get_ingested_file_list(*args, **kwargs):
    url = kwargs['base_api_host']
    jwt = kwargs['jwt_token']
    path = '/ingestion/users/filenames'

    headers = {'Authorization': f'Bearer {jwt}'}
    res = requests.get(url + path, headers=headers)
    if res.status_code != 200:
        print(f'Error: {res}')
        raise Exception('Unable to fetch filename list')

    filenames = [item['file_name'] for item in res.json()['models']]
    print(f'Files in base-api: {filenames}')
    return filenames


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


def compare_ingested_files_with_stored_files(*args, **kwargs):
    ingested_files = kwargs['ti'].xcom_pull(task_ids='get_ingested_file_list')
    sftp_files_path = kwargs['src_file_path']
    diff_file_path = kwargs['diff_file_path']

    print(f'ingested_files: {ingested_files}')
    print(f'sftp_files_path: {sftp_files_path}')

    f = open(sftp_files_path, 'r')
    sftp_files = list(map(lambda x: x.strip(), f.readlines()))
    f.close()

    seen = defaultdict(bool)
    for name in ingested_files:
        seen[name] = True

    files_to_ingest = []
    for name in sftp_files:
        if not seen[name]:
            files_to_ingest.append(name)

    print(f'Files to ingest: {files_to_ingest}')

    f = open(diff_file_path, 'w')
    for name in files_to_ingest:
        f.write(f'{name}{linesep}')
    f.close()

    return files_to_ingest


t0 = PythonOperator(
    task_id='ready_for_ingestion',
    python_callable=be_ready,
    op_kwargs=params,
    dag=dag)

# Get sftp server list
cmd0 = 'eval sync_sftp {{params.SFTP_HOST}} {{params.SFTP_PORT}} {{params.SFTP_USER}} {{params.SFTP_PASSWORD}} list' \
    '> {{params.intermediate_file_store}}'
t1_pre = BashOperator(
    task_id='run_sftp_script_to_get_list',
    bash_command=cmd0,
    retries=2,
    dag=dag)

# Extract only .csv
cmd1 = 'cat {{params.intermediate_file_store}}' \
    '| grep -v sftp' \
    '| grep .csv' \
    '| sort -i' \
    '> {{params.src_file_path}}' \
    '&& cat {{params.src_file_path}}'
t1_post = BashOperator(
    task_id='extract_csv_file_list',
    bash_command=cmd1,
    xcom_push=True,
    retries=3,
    dag=dag)

# Get ingested file list
t2 = PythonOperator(
    task_id='get_ingested_file_list',
    python_callable=get_ingested_file_list,
    op_kwargs=params,
    dag=dag)

# Store diff files
t3 = PythonOperator(
    task_id='compare_ingested_files_with_stored_files',
    python_callable=compare_ingested_files_with_stored_files,
    provide_context=True,
    op_kwargs=params,
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


t0 >> [t1_pre, t2]
t1_pre >> t1_post
[t1_post, t2] >> t3 >> t4 >> t5 >> t6
