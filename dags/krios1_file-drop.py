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

import logging
LOG = logging.getLogger(__name__)


###
# default args
###
args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': utils.dates.days_ago(0),
    'configuration_file': '/srv/cryoem/tem1-experiment.yaml',
    'source_directory': '/srv/cryoem/tem1/',
    'source_fileglob': '**/*.*',
    'destination_directory': '/usr/local/airflow/data/',
    'remove_files_after': 720,
}


###
# grab some experimental details that we need ###
###
def parseConfigurationFile(**kwargs):
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
            
            

with DAG( 'cryoem_krios1_file-drop',
        description="Monitor for new cryoem for krios1 metadata and data and put it into long term storage",
        schedule_interval="* * * * *",
        default_args=args,
        max_active_runs=1
    ) as dag:
   
   
    ###
    # read and pase the configuration file for the experiment
    ###
    t_config = EnsureConfigurationExistsSensor(task_id='parse_config',
        configuration_file=args['configuration_file'],
        destination_directory=args['destination_directory'],
    )


    ###
    # rsync the files over to perm storage
    ###
    t_rsync = BashOperator( task_id='rsync_files',
        bash_command = "echo rsync -av {{ params.source_directory }} {{ ti.xcom_pull(task_ids='parse_config',key='experiment_directory') }}",
        params={ 'source_directory': args['source_directory'] }
    )


    ###
    # delete files large file over a certain amount of time
    ###
    t_remove = BashOperator( task_id='remove_old_source_files',
        bash_command="echo find {{ params.source_directory }} -name \"{{ params.file_glob }}\" -type f -mmin +{{ params.age }} -exec rm -vf '{}' +",
        params={ 
            'source_directory': args['source_directory'],
            'file_glob': '*.mrc',
            'age': args['remove_files_after'],
        }
    )

    ###
    # define pipeline
    ###
    t_config >> t_rsync >> t_remove







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
