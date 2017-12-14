'''
This DAG listens for files that should be dropped onto a mountpoint that is accessible to the airflow workers. It will then stage the file to another location for permanent storage. After this, it will trigger a few other DAGs in order to complete processing.
'''

from airflow import DAG

from airflow import settings
import contextlib

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.operators import EnsureDirectoryExistsOperator, RsyncOperator, EnsureConfigurationExistsSensor

from airflow.exceptions import AirflowException, AirflowSkipException


from airflow.models import DagRun, DagBag
from airflow.utils.state import State

from pathlib import Path
from datetime import datetime
from time import sleep
import re

import logging
LOG = logging.getLogger(__name__)


###
# default args
###
args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': datetime(2017,1,1), 
    'configuration_file': '/srv/cryoem/experiment/tem1/tem1-experiment.yaml',
    'source_directory': '/srv/cryoem/tem1/',
    'source_includes': [ 'Atlas*', 'FoilHole_*_Data_*.jpg', 'FoilHole_*_Data_*.xml', 'FoilHole_*_Data_*.mrc', 'FoilHole_*_Data_*.dm4', \
        '*.tif', 'CountRef_*.dm4', '*_g6*.mrc', '*_g6*.mrc.mdoc', \
	'*_tomo*.mrc', '*_trial.mrc', '*_map.mrc', '*_-0.0.mrc', '*.mdoc', '*.mrc.anchor', '*.bak', '*.log', '*.nav', '*.png', '*.txt' ],
    'destination_directory': '/gpfs/slac/cryo/fs1/exp/',
    'remove_files_after': 360, # minutes
    'remove_files_larger_than': '+100M',
    'trigger_preprocessing': False, #True,
    'dry_run': False,
}


def trigger_preprocessing(context):
    """ calls the preprocessing dag: pass the filenames of the stuff to process """
    # we have a jpg, xml, small mrc and large mrc, and gainref dm4 file
    # assume the common filename is the same and allow the  preprocessing dag wait for the other files? what happens if two separate calls to the same dag occur?
    found = {}
    if context == None:
        return

    for f in context['ti'].xcom_pull( task_ids='rsync_data', key='return_value' ):
        this = Path(f).resolve().stem
        for pattern in ( r'\-\d+$', r'\-gain\-ref$' ):
            if re.search( pattern, this):
                this = re.sub( pattern, '', this)
            # LOG.warn("mapped: %s -> %s" % (f, this))
        #LOG.info("this: %s, f: %s" % (this,f))
        # EPU: only care about the xml file for now (let the dag deal with the other files
        if f.endswith('.xml') and not f.startswith('Atlas'):
            #LOG.warn("found EPU metadata %s" % this )
            found[this] = True
        # serialEM: just look for tifs
        elif f.endswith('.tif'):
            m = re.match( r'^(?P<base>.*\__\d\d\d\d)\_.*\.tif$', f )
            if m:
                #LOG.info('found %s' % (m.groupdict()['base'],) )
                found[m.groupdict()['base']] = True

    for base_filename,_ in found.items():
        exp = context['ti'].xcom_pull( task_ids='parse_config', key='experiment')
        run_id = '%s_%s__%s' % (exp['name'], exp['microscope'], base_filename)
        dro = DagRunOrder(run_id=run_id) 
        d = { 
            'directory': context['ti'].xcom_pull( task_ids='parse_config', key='experiment_directory'),
            'base': base_filename,
            'experiment': exp['name'],
            'microscope': exp['microscope'],
            'fmdose': exp['fmdose'],
        }
        LOG.info('triggering dag %s with %s' % (run_id,d))
        # now = this
        dro.payload = d
        # implement dry_run somehow
        yield dro
    return
    

def trigger_null(context):
    raise AirflowSkipException('Intentionally not doing it') 

@contextlib.contextmanager
def create_session():
    session = settings.Session()
    try:
        yield session
        session.expunge_all()
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

class TriggerMultipleDagRunOperator(TriggerDagRunOperator):
    template_fields = ('trigger_dag_id',)
    def execute(self, context):
        count = 0
        for dro in self.python_callable(context):
            # LOG.info("context: %s" % (context,))
            # LOG.info(' TRIGGER %s' % (self.trigger_dag_id,))
            if dro and ( not 'dry_run' in context['params'] or ( 'dry_run' in context['params'] and not context['params']['dry_run']) ):
                with create_session() as session:
                    dbag = DagBag(settings.DAGS_FOLDER)
                    trigger_dag = dbag.get_dag(self.trigger_dag_id)
                    dr = trigger_dag.create_dagrun(
                        run_id=dro.run_id,
                        state=State.RUNNING,
                        conf=dro.payload,
                        external_trigger=True)
                    # LOG.info("Creating DagRun %s", dr)
                    session.add(dr)
                    session.commit() 
                    count = count + 1
            else:
                LOG.info("Criteria not met, moving on")
        if count == 0:
            raise AirflowSkipException('No external dags triggered')

with DAG( 'tem1_file-drop',
        description="Monitor for new cryoem for krios1 metadata and data and put it into long term storage",
        schedule_interval="* * * * *",
        default_args=args,
        catchup=False,
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
    ###
    t_exp_dir = BashOperator( task_id='ensure_directory',
        bash_command = "timeout 5 mkdir -p {{ ti.xcom_pull(task_ids='parse_config',key='experiment_directory') }}/raw/"
    )

    ###
    # rsync the globbed files over and store on target without hierachy
    ###
    t_rsync = RsyncOperator( task_id='rsync_data',
        dry_run=args['dry_run'],
        source=args['source_directory'],
        target="{{ ti.xcom_pull(task_ids='parse_config',key='experiment_directory') }}/raw/",
        includes=args['source_includes'],
        prune_empty_dirs=True,
        chmod='ug+x,u+rw,g+r,g-w,o-rwx',
        flatten=False,
        priority_weight=50,
    )

    ###
    # delete files large file over a certain amount of time
    ###
    t_remove = BashOperator( task_id='remove_old_source_files',
        bash_command="find {{ params.source_directory }} {{ params.file_glob }} -type f -mmin +{{ params.age }} -size {{ params.size }} -exec {{ params.dry_run }} rm -vf '{}' +",
        params={ 
            'source_directory': args['source_directory'],
            'file_glob': "\( -name 'FoilHole_*_Data_*.mrc' -o -name 'FoilHole_*_Data_*.dm4' -o -name '*.tif' \)",
            'age': args['remove_files_after'],
            'size': args['remove_files_larger_than'],
            'dry_run': 'echo' if args['dry_run'] else '',
        }
    )


    ###
    # trigger another daq to handle the rest of the pipeline
    ###
    t_trigger_preprocessing = TriggerMultipleDagRunOperator( task_id='trigger_preprocessing',
        trigger_dag_id="{{ ti.xcom_pull( task_ids='parse_config', key='experiment' )['name'] }}_{{ ti.xcom_pull( task_ids='parse_config', key='experiment' )['microscope'] }}",
        python_callable=trigger_preprocessing,
        params={
            'dry_run': False if args['trigger_preprocessing'] and not args['dry_run'] else True,
        }
    )


    ###
    # define pipeline
    ###
    t_config >> t_exp_dir >> t_rsync >> t_remove
    # t_config >> t_rsync
    t_rsync >> t_trigger_preprocessing


