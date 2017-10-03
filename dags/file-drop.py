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




def conditional_trigger(context, dag_run_obj, parameter='conditional_param'):
    """ conditionally determine whether to trigger a dag """
    LOG.debug('controller dag parameters: %s' % (context['params']))
    if context['params']['filename']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return dag_run_obj

        
taskname_waitForFile = 'wait_for_new_file'

### wait for a file to show up ###
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

taskname_archiveFileLocation = 'ensure_file_not_exist'

### make sure we're not overwriting anything ###
def ensureFileDoesNotExist(**kwargs):
    orig_filename = Path(kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_waitForFile))
    f = Path( finalFilePath( kwargs['directory'], orig_filename.name, kwargs['experiment_id'] ) )
    # kwargs['task_instance'].xcom_push(key='filepath',value=f)
    kwargs['task_instance'].xcom_push(key='filepath',value=f)
    if f.exists():
        return False
    return f
t_dst_file = ShortCircuitOperator(task_id=taskname_archiveFileLocation, dag=dag,
    python_callable=ensureFileDoesNotExist,
    xcom_push=True,
    op_kwargs={ 'directory': args['destination_directory'], 'experiment_id': args['experiment_id'] }
)

### move the file to the new location ###
def moveFile(**kwargs):
    # get source from xcom
    src = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_waitForFile) 
    if not src:
        raise AirflowException('No source file defined from task_instance %s' % kwargs['task_instance'])
    dst = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_archiveFileLocation)
    if not dst:
        raise AirflowException('No destination file defined from task_instance %s' % kwargs['task_instance'])
    # ensure 
    try:
        LOG.info("moving %s to %s" % (src,dst))
        shutil.move(src,dst)
    except Exception as e:
        raise AirflowException('Error moving file: %s' % (e,))
    return dst    
t_stage = PythonOperator(task_id='move_file', dag=dag,
    python_callable=moveFile,
    provide_context=True,
    op_kwargs={'destination_directory': args['destination_directory'] }
)


t_trigger = TriggerDagRunOperator(task_id='trigger_remote_dag',
    trigger_dag_id='remote_dag_to_run',
    python_callable=conditional_trigger,
    params={'conditional_param': True, 'message': 'hellllloooo...'},
    dag=dag
)


DummyOperator(task_id='motioncor2')
DummyOperator(task_id='trigger_ctf')

# slack = SlackAPIPostOperator(
#         task_id='speedmap_slack',
#         channel="#airflow-test",
#         token=Variable.get('slack_token'),
#         params={'map': speedmap},
#         text='',
#         dag=dag)

t_wait >> t_dir >> t_dst_file >> t_stage >> t_trigger