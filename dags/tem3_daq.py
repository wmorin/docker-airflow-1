'''
This DAG listens for files that should be dropped onto a mountpoint that is accessible to the airflow workers. It will then stage the file to another location for permanent storage. After this, it will trigger a few other DAGs in order to complete processing.
'''

from airflow import DAG

from airflow import settings
import contextlib
from airflow.utils.decorators import apply_defaults

from airflow.operators.dummy_operator import DummyOperator

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator

from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder

from airflow.operators import LogbookConfigurationSensor
from airflow.operators import RsyncOperator, ExtendedAclOperator

# from subprocess import Popen, STDOUT, PIPE, call
# from tempfile import gettempdir, NamedTemporaryFile
# from airflow.utils.file import TemporaryDirectory

from airflow.operators import TriggerMultipleDagRunOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from airflow.hooks.http_hook import HttpHook

from airflow.models import DagRun, DagBag
from airflow.utils.state import State


from pathlib import Path
from datetime import datetime
from time import sleep
import re
import os
# from ast import literal_eval

import logging
LOG = logging.getLogger(__name__)

###
# default args
###
args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': datetime(2018,1,1), 
    'tem': 'TEM3',
    'source_directory': '/srv/cryoem/tem3/',
    'source_excludes':  [ '*.bin', ],
    'destination_directory': '/gpfs/slac/cryo/fs1/exp/',
    'logbook_connection_id': 'cryoem_logbook',
    'remove_files_after': 1540, # minutes
    'remove_files_larger_than': '+100M',
    'dry_run': False,
}



with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Stream data off the TEMs based on the CryoEM logbook",
        schedule_interval="* * * * *",
        default_args=args,
        catchup=False,
        max_active_runs=1
    ) as dag:
   
   
   
    logbook_hook = HttpHook( http_conn_id=args['logbook_connection_id'], method='GET' )
   
    ###
    # read and pase the configuration file for the experiment
    ###
    config = LogbookConfigurationSensor(task_id='config',
        http_hook=logbook_hook,
        microscope=args['tem'],
        base_directory=args['destination_directory']
    )

    ###
    # make sure we have a directory to write to; also create the symlink
    ###
    sample_directory = BashOperator( task_id='sample_directory',
        bash_command = "timeout 5 mkdir -p {{ ti.xcom_pull(task_ids='config',key='experiment_directory') }}/{{ ti.xcom_pull(task_ids='config',key='sample')['guid'] }}/raw/ && echo {{ ti.xcom_pull(task_ids='config',key='experiment_directory') }}/{{ ti.xcom_pull(task_ids='config',key='sample')['guid'] }}/raw/",
        xcom_push=True
    )
    
    sample_symlink = BashOperator( task_id='sample_symlink',
        bash_command = """
        cd {{ ti.xcom_pull(task_ids='config',key='experiment_directory') }}/
        if [ ! -L {{ ti.xcom_pull(task_ids='config',key='sample')['name'] }}  ]; then
            ln -s {{ ti.xcom_pull(task_ids='config',key='sample')['guid'] }} {{ ti.xcom_pull(task_ids='config',key='sample')['name'] }} 
        fi
        """
    )

    setfacl = ExtendedAclOperator( task_id='setfacl',
        directory="{{ ti.xcom_pull(task_ids='config',key='experiment_directory') }}",
        users="{{ ti.xcom_pull(task_ids='config',key='collaborators') }}",
    )



    last_rsync = BashOperator( task_id='last_rsync',
        bash_command="""
        if [ ! -d {{ params.directory }} ]; then
          mkdir {{ params.directory }}
        fi
        cd {{ params.directory }}
        ls -1 -t -A {{ params.prefix }}*  2>/dev/null | tail -n1 | xargs -n1 --no-run-if-empty readlink -e 
        """,
        params={
            'directory': args['destination_directory'] + '/.daq/',
            'prefix': args['tem'] + '_sync_'
        },
        xcom_push=True,
    )

    ###
    # use this file as an anchor for which files to care about
    ###
    touch = BashOperator( task_id='touch',
        bash_command="""
        mkdir {{ params.directory }}
        cd {{ params.directory }}
        TS=$(date '+%Y%m%d_%H%M%S')
        touch {{ params.prefix }}${TS}
        """,
        params={
            'directory': args['destination_directory'] + '/.daq/',
            'prefix': args['tem'] + '_sync_'
        }
    )

    ###
    # rsync the globbed files over and store on target without hierachy
    ###
    rsync = RsyncOperator( task_id='rsync',
        dry_run=str(args['dry_run']),
        source=args['source_directory'],
        target="{{ ti.xcom_pull(task_ids='sample_directory') }}",
        excludes=args['source_excludes'],
        prune_empty_dirs=True,
        chmod='ug+x,u+rw,g+r,g-w,o-rwx',
        flatten=False,
        priority_weight=50,
        newer="{{ ti.xcom_pull(task_ids='last_rsync') }}"
    )

    untouch = BashOperator( task_id='untouch',
        bash_command="""
        cd {{ params.directory }}
        if [ `ls -1 -t -A {{ params.prefix }}* | wc -l` -gt 1 ]; then
          ls -1 -t -A {{ params.prefix }}* | tail -n +2 | xargs -t rm -f
        fi
        """,
        params={
            'directory': args['destination_directory'] + '.daq/',
            'prefix': args['tem'] + '_sync_'
        }
    )

    # ###
    # # delete files large file over a certain amount of time
    # ###
    # delete = BashOperator( task_id='delete',
    #     bash_command="find {{ params.source_directory }} {{ params.file_glob }} -type f -mmin +{{ params.age }} -size {{ params.size }} -exec {% if ti.xcom_pull(task_ids='parse_config',key='dry_run') %}echo{% endif %} rm -vf '{}' +",
    #     params={
    #         'source_directory': args['source_directory'],
    #         'file_glob': "\( -name 'FoilHole_*_Data_*.mrc' -o -name 'FoilHole_*_Data_*.dm4' -o -name '*.tif' \)",
    #         'age': args['remove_files_after'],
    #         'size': args['remove_files_larger_than'],
    #         #'dry_run': 'echo' if args['dry_run'] else '',
    #     }
    # )
    #
    #
    ###
    # trigger another daq to handle the rest of the pipeline
    ###
    trigger = TriggerMultipleDagRunOperator( task_id='trigger',
        trigger_dag_id="{{ ti.xcom_pull( task_ids='config', key='experiment' ) }}",
        dry_run=str(args['dry_run']),
    )


    ###
    # define pipeline
    ###
    config >> sample_directory >> touch >> rsync >> untouch
    sample_directory >> sample_symlink
    config >> last_rsync >> rsync >> trigger
    sample_directory >> setfacl
    
    # untouch >> delete
    