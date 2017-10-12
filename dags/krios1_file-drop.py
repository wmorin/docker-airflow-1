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

import yaml


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
    'configuration_file': '/srv/cryoem/tem1-experiment.yaml',
    'source_directory': '/srv/cryoem/tem1/',
    'source_fileglob': '**/*.*',
    'destination_directory': '/usr/local/airflow/data/',
}

dag = DAG( 'cryoem_krios1_file-drop',
    description="Monitor for new cryoem for krios1 metadata and data and put it into long term storage",
    schedule_interval="* * * * *",
    default_args=args,
    max_active_runs=1
)


###
# grab some experimental details that we need ###
###
def parseConfigurationFile(**kwargs):
    LOG.info("Checking config file %s" % (kwargs['configuration_file'],))
    with open( kwargs['configuration_file'], 'r' ) as stream:
        try:
            d = yaml.load(stream)
            for k,v in d.items():                
                kwargs['task_instance'].xcom_push(key=k,value=v)
            exp_dir = '%s_%s' % (d['experiment']['name'], d['experiment']['microscope'])
            kwargs['task_instance'].xcom_push(key='experiment_directory', value='%s/%s/' % ( kwargs['destination_directory'], exp_dir))
        except Exception as e:
            raise AirflowException('Error creating destination directory: %s' % (e,))
    return kwargs['configuration_file']
class EnsureConfigurationExistsSensor(ShortCircuitOperator):
    """ monitor for configuration defined on tem """
    def __init__(self,configuration_file,destination_directory,*args,**kwargs):
        super(EnsureConfigurationExistsSensor,self).__init__(
            python_callable=parseConfigurationFile, 
            op_kwargs={ 'configuration_file': configuration_file, 'destination_directory': destination_directory }, 
            *args, **kwargs)
t_config = EnsureConfigurationExistsSensor(task_id='configuration_file', dag=dag,
    configuration_file=args['configuration_file'],
    destination_directory=args['destination_directory'],
)


###
# wait for a file to show up
###
taskname_waitForFile = 'wait_for_new_file'
t_wait = FileGlobSensor(task_id=taskname_waitForFile, dag=dag,
    soft_fail=True,
    timeout=1,
    directory=args['source_directory'],
    pattern=args['source_fileglob'],
    recursive=True,
)



t_rsync = BashOperator( task_id='rsync_files', dag=dag,
    bash_command = "echo rsync -av {{ params.source_directory }} {{ ti.xcom_pull(task_ids='configuration_file',key='experiment_directory') }}",
    params={ 'source_directory': args['source_directory'] }
)


    
###
# task to work out the source and destination file names
###
def ensureExperimentDirectories(**kwargs):
    root_dst_dir = kwargs['task_instance'].xcom_pull(key='experiment_directory', task_ids='configuration_file')
    files = kwargs['task_instance'].xcom_pull(key='files', task_ids=taskname_waitForFile)
    # ensure that the full path on destination is created
    dirs = set([ '%s/%s' % (root_dst_dir, os.path.dirname(f)) for f in files ])
    for d in dirs:
        if not os.path.exists(d):
            try:
                LOG.info("creating dir: %s" % (d,))
                os.makedirs(d)
            except Exception as e:
                raise AirflowException('Error creating destination directory %s: %s' % (d,e,))            
t_exp_dirs = PythonOperator( task_id='ensure_directories', dag=dag,
    python_callable=ensureExperimentDirectories,
    op_kwargs={ 'destination_directory': args['destination_directory'] }
) 


###
# copy the files over
###
### move the file to the new location ###
def copyFiles(**kwargs):
    root_dst_dir = kwargs['task_instance'].xcom_pull(key='experiment_directory', task_ids='configuration_file')
    for f in kwargs['task_instance'].xcom_pull(key='files', task_ids=taskname_waitForFile):
        try:
            src = '%s/%s' % (kwargs['source_directory'], f )
            dst = '%s/%s/' % (root_dst_dir,os.path.dirname(f))
            LOG.info("copying %s to %s" % (src,dst))
            shutil.copy2(src,dst)
        except Exception as e:
            raise AirflowException('Error moving file: %s' % (e,))
t_copy = PythonOperator(task_id='copy_files', dag=dag,
    python_callable=copyFiles,
    op_kwargs={ 'source_directory': args['source_directory'] }
)

###
# delete the source files
###

# only delete mrc files? how to ignore copy next time? just rsync?
# delete files only after a while? ie find +mtime

def deleteSourceFiles(**kwargs):
    for f in kwargs['task_instance'].xcom_pull(key='files', task_ids=taskname_waitForFile):
        try:
            src = '%s/%s' % (kwargs['source_directory'],f)
            LOG.info("deleting file %s" % (src,))
            # os.remove(src)
        except Exception as e:
            raise AirflowException('Error deleting file: %s' % (e,))
t_delete_source = PythonOperator(task_id='delete_source_files', dag=dag,
    python_callable=deleteSourceFiles,
    op_kwargs={ 'source_directory': args['source_directory'] }
)



t_remove_old_source = BashOperator( task_id='remove_old_source_files', dag=dag,
    bash_command="echo find {{ params.source_directory }} -name '{{ params.extension }}' -type f -mmin +{{ params.age }} -exec rm -vf '{}' +",
    params={ 
        'source_directory': args['source_directory'],
        'file': '*.mrc',
        'age': 720,
    }
)

### TODO: ensure we have permissions ###
# t_dir = EnsureDirectoryExistsFromOperator(task_id='ensure_destination_directory', dag=dag,
#     from_task_id=taskname_waitForFile,
# )
#
# ### make sure we're not overwriting anything ###
# taskname_archiveFileLocation = 'ensure_file_not_exist'
# def ensureFileDoesNotExist(**kwargs):
#     orig_filename = Path(kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_waitForFile))
#     exp = kwargs['task_instance'].xcom_pull(key=None, task_ids='configuration_file')
#     f = Path( finalFilePath( kwargs['directory'], orig_filename.name, exp['name'], exp['microscope'] ) )
#     kwargs['task_instance'].xcom_push(key='filepath',value=f)
#     if f.exists():
#         return False
#     return f
# t_dst_file = ShortCircuitOperator(task_id=taskname_archiveFileLocation, dag=dag,
#     python_callable=ensureFileDoesNotExist,
#     xcom_push=True,
#     op_kwargs={ 'directory': args['destination_directory'] }
# )
#
#
# def checksumFile(fname,chunksize=4096):
#     with open(fname, "rb") as f:
#         for chunk in iter(lambda: f.read(chunksize), b""):
#             hash_md5.update(chunk)
#     return hash_md5.hexdigest()
#
# ### generate a checksum for the src ###
# def srcChecksum(**kwargs):
#     src = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_waitForFile)
#     md5 = checksumFile(src)
#     kwargs['task_instance'].xcom_push(key='src_checksum',value=md5)
#     return md5
# t_src_checksum = PythonOperator(task_id='calc_src_checksum', dag=dag,
#     python_callable=srcChecksum
# )
#

#
# ### generate a checksum for the dst ###
# def dstChecksum(**kwargs):
#     dst = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_archiveFileLocation)
#     md5 = checksumFile(dst)
#     kwargs['task_instance'].xcom_push(key='dst_checksum',value=md5)
#     return md5
# t_dst_checksum = PythonOperator(task_id='calc_dst_checksum', dag=dag,
#     python_callable=dstChecksum
# )
#
#
# def ensureChecksumSame(**kwargs):
#     src_md5 = kwargs['task_instance'].xcom_pull(key=None, task_ids='calc_src_checksum')
#     dst_md5 = kwargs['task_instance'].xcom_pull(key=None, task_ids='calc_dst_checksum')
#     LOG.info("source md5: %s, dst md5: %s" % (src_md5,dst_md5))
#     if src_md5 != dst_md5:
#         raise AirflowException('Source and destination file checksums do not match!')
#     return True
# t_ensure_checksum = PythonOperator(task_id='ensure_file_checksums', dag=dag,
#     python_callable=ensureChecksumSame,
# )
#
#
# ### delete the source file ###
# def deleteSourceFile(**kwargs):
#     src = kwargs['task_instance'].xcom_pull(key=None, task_ids=taskname_waitForFile)
#     try:
#         LOG.info("deleting file %s" % (src,))
#         os.remove(src)
#     except Exception as e:
#         raise AirflowException('Error deleting file: %s' % (e,))
#     return src
# t_delete_source = PythonOperator(task_id='delete_source', dag=dag,
#     python_callable=deleteSourceFile,
#     provide_context=True
# )
#
# # this shoudl probably be a branching depending on whether its tif or mrc
# t_mrc2jpg = DummyOperator(task_id='get_jpg_from_mrc', dag=dag
# )
#
#
# ### other dags ###
# t_tif2mrc = DummyOperator(task_id='trigger_tif2mrc_dag', dag=dag
# )
# t_motioncor2 = DummyOperator(task_id='trigger_motioncor2_dag', dag=dag
# )
# t_ctffind = DummyOperator(task_id='trigger_ctf_dag', dag=dag
# )
#


### define task dependencies ###
#t_config >> t_wait >> t_exp_dirs >> t_copy >> t_delete_source

t_config >> t_rsync >> t_remove_old_source




#t_wait >> t_dir >> t_dst_file >> t_src_checksum >> t_copy
#t_copy >> t_mrc2jpg 
#t_copy >> t_dst_checksum >> t_ensure_checksum >> t_delete_source
#t_ensure_checksum >> t_motioncor2
#t_ensure_checksum >> t_ctffind
#t_ensure_checksum >> t_tif2mrc





### MSIC ###

# def conditional_trigger(context, dag_run_obj, parameter='conditional_param'):
#     """ conditionally determine whether to trigger a dag """
#     LOG.debug('controller dag parameters: %s' % (context['params']))
#     if context['params']['filename']:
#         dag_run_obj.payload = {'message': context['params']['message']}
#         return dag_run_obj
#
#
#
# ### trigger other dags for further pre-processing ###
# t_trigger = TriggerDagRunOperator(task_id='trigger_remote_dag',
#     trigger_dag_id='remote_dag_to_run',
#     python_callable=conditional_trigger,
#     params={'conditional_param': True, 'message': 'hellllloooo...'}
# )


# slack = SlackAPIPostOperator(
#         task_id='speedmap_slack',
#         channel="#airflow-test",
#         token=Variable.get('slack_token'),
#         params={'map': speedmap},
#         text='',
#         dag=dag)
