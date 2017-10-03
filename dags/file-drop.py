'''
This DAG listens for files that should be dropped onto a mountpoint that is accessible to the airflow workers. It will then stage the file to another location for permanent storage. After this, it will trigger a few other DAGs in order to complete processing.
'''



from airflow import utils
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators import FileGlobSensor, FileOperator, FileGlobExistsOperator, EnsureDirectoryExistsOperator
from airflow.exceptions import AirflowException

from datetime import datetime
import os
import shutil
from pathlib import Path


import logging
LOG = logging.getLogger(__name__)

import hashlib
hash_md5 = hashlib.md5()


args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': utils.dates.days_ago(0),
    'tem': 1,
    'experiment_id': 'abc123',
    'source_directory': '/srv/cryoem/tem3/DoseFractions',
    'source_fileglob': '*.dummy',
    'destination_directory': '/usr/local/airflow/data',
}

dag = DAG( 'cryoem_filedrop',
    description="Monitor for new cryoem metadata and data and put it into long term storage",
    schedule_interval="* * * * *",
    default_args=args,
    max_active_runs=1
)



### wait for a file to show up ###        
taskname_waitForFile = 'wait_for_new_file'
t_wait = FileGlobExistsOperator(task_id=taskname_waitForFile, dag=dag,
    directory=args['source_directory'],
    pattern=args['source_fileglob']
)


def finalFilePath(directory, filename, experiment):
    # TODO: add experiment and tranform of filename
    return '%s/%s' % (directory,filename)

### make sure the destination directory exists ###
### TODO: ensure we have permissions ###
t_dir = EnsureDirectoryExistsOperator(task_id='ensure_destination_directory', dag=dag,
    directory=args['destination_directory']
)


### make sure we're not overwriting anything ###
taskname_archiveFileLocation = 'ensure_file_not_exist'
def ensureFileDoesNotExist(**kwargs):
    orig_filename = Path(kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_waitForFile))
    f = Path( finalFilePath( kwargs['directory'], orig_filename.name, kwargs['experiment_id'] ) )
    kwargs['task_instance'].xcom_push(key='filepath',value=f)
    if f.exists():
        return False
    return f
t_dst_file = ShortCircuitOperator(task_id=taskname_archiveFileLocation, dag=dag,
    python_callable=ensureFileDoesNotExist,
    xcom_push=True,
    op_kwargs={ 'directory': args['destination_directory'], 'experiment_id': args['experiment_id'] }
)


def checksumFile(fname,chunksize=4096):
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(chunksize), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
    
### generate a checksum for the src ###
def srcChecksum(**kwargs):
    src = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_waitForFile) 
    md5 = checksumFile(src)
    kwargs['task_instance'].xcom_push(key='src_checksum',value=md5)
    return md5
t_src_checksum = PythonOperator(task_id='calc_src_checksum', dag=dag,
    python_callable=srcChecksum
)

### move the file to the new location ###
def copyFile(**kwargs):
    # get source from xcom
    src = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_waitForFile) 
    if not src:
        raise AirflowException('No source file defined from task_instance %s' % kwargs['task_instance'])
    dst = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_archiveFileLocation)
    if not dst:
        raise AirflowException('No destination file defined from task_instance %s' % kwargs['task_instance'])
    # ensure 
    try:
        LOG.info("copying %s to %s" % (src,dst))
        shutil.copy2(src,dst)
    except Exception as e:
        raise AirflowException('Error moving file: %s' % (e,))
    return dst
t_stage = PythonOperator(task_id='copy_file', dag=dag,
    python_callable=copyFile,
    provide_context=True
)

### generate a checksum for the dst ###
def dstChecksum(**kwargs):
    dst = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_archiveFileLocation) 
    md5 = checksumFile(dst)
    kwargs['task_instance'].xcom_push(key='dst_checksum',value=md5)
    return md5
t_dst_checksum = PythonOperator(task_id='calc_dst_checksum', dag=dag,
    python_callable=dstChecksum
)


def ensureChecksumSame(**kwargs):
    src_md5 = kwargs['task_instance'].xcom_pull(key=None, task_ids='calc_src_checksum')
    dst_md5 = kwargs['task_instance'].xcom_pull(key=None, task_ids='calc_dst_checksum')
    LOG.info("source md5: %s, dst md5: %s" % (src_md5,dst_md5))
    if src_md5 != dst_md5:
        raise AirflowException('Source and destination file checksums do not match!')
    return True
t_ensure_checksum = PythonOperator(task_id='ensure_file_checksums', dag=dag,
    python_callable=ensureChecksumSame,
)


### delete the source file ###
def deleteSourceFile(**kwargs):
    src = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_waitForFile) 
    try:
        LOG.info("deleting file %s" % (src,))
        os.remove(src)
    except Exception as e:
        raise AirflowException('Error deleting file: %s' % (e,))
    return src
t_delete_source = PythonOperator(task_id='delete_source', dag=dag,
    python_callable=deleteSourceFile,
    provide_context=True
)

### other dags ###
t_motioncor2 = DummyOperator(task_id='trigger_motioncor2', dag=dag
)
t_ctffind = DummyOperator(task_id='trigger_ctf', dag=dag
)



### define task dependencies ###
t_wait >> t_dir >> t_dst_file >> t_src_checksum >> t_stage 
t_stage >> t_dst_checksum >> t_ensure_checksum >> t_delete_source
t_ensure_checksum >> t_motioncor2
t_ensure_checksum >> t_ctffind





### MSIC ###

def conditional_trigger(context, dag_run_obj, parameter='conditional_param'):
    """ conditionally determine whether to trigger a dag """
    LOG.debug('controller dag parameters: %s' % (context['params']))
    if context['params']['filename']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return dag_run_obj



### trigger other dags for further pre-processing ###
t_trigger = TriggerDagRunOperator(task_id='trigger_remote_dag',
    trigger_dag_id='remote_dag_to_run',
    python_callable=conditional_trigger,
    params={'conditional_param': True, 'message': 'hellllloooo...'}
)


# slack = SlackAPIPostOperator(
#         task_id='speedmap_slack',
#         channel="#airflow-test",
#         token=Variable.get('slack_token'),
#         params={'map': speedmap},
#         text='',
#         dag=dag)
