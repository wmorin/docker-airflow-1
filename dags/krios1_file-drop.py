'''
This DAG listens for files that should be dropped onto a mountpoint that is accessible to the airflow workers. It will then stage the file to another location for permanent storage. After this, it will trigger a few other DAGs in order to complete processing.
'''

from airflow import utils
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators import EnsureDirectoryExistsOperator, RsyncOperator, EnsureConfigurationExistsSensor

from airflow.exceptions import AirflowException

import logging
LOG = logging.getLogger(__name__)


###
# default args
###
args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': utils.dates.days_ago(0),
    'configuration_file': '/srv/cryoem/experiment/tem1/tem1-experiment.yaml',
    'source_directory': '/srv/cryoem/tem1/',
    'source_fileglob': ['**/FoilHole_*_Data_*.jpg','**/FoilHole_*_Data_*.xml','**/FoilHole_*_Data_*.dm4','**/FoilHole_*_Data_*.mrc'],
    'destination_directory': '/gpfs/slac/cryo/fs1/exp/',
    'remove_files_after': 540, # minutes
}

            
            

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
    # create the experimental directory
    t_exp_dir = BashOperator( task_id='ensure_directory',
        bash_command = "timeout 5 mkdir -p {{ ti.xcom_pull(task_ids='parse_config',key='experiment_directory') }}"
    )

    ###
    # rsync the globbed files over and store on target without hierachy
    ###
    t_rsync = RsyncOperator( task_id='rsync_data',
        dry_run=True,
        source=args['source_directory'] + args['source_fileglob'] if isinstance(args['source_fileglob'], str ) else ' '.join( [ '%s%s'% (args['source_directory'],f) for f in args['source_fileglob'] ] ),
        target="{{ ti.xcom_pull(task_ids='parse_config',key='experiment_directory') }}",
        excludes="{{ ti.xcom_pull(task_ids='parse_config',key='excludes') }}",
        prune_empty_dirs=True,
        flatten=True,
    )

    ###
    # delete files large file over a certain amount of time
    ###
    t_remove = BashOperator( task_id='remove_old_source_files',
        bash_command="find {{ params.source_directory }} -name \"{{ params.file_glob }}\" -type f -mmin +{{ params.age }} -size {{ params.size }} -exec rm -vf '{}' +",
        params={ 
            'source_directory': args['source_directory'],
            'file_glob': 'FoilHole*.mrc',
            'age': args['remove_files_after'],
            'size': '+100M',
        }
    )

    ###
    # define pipeline
    ###
    t_config >> t_exp_dir >> t_rsync >> t_remove



