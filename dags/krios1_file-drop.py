'''
This DAG listens for files that should be dropped onto a mountpoint that is accessible to the airflow workers. It will then stage the file to another location for permanent storage. After this, it will trigger a few other DAGs in order to complete processing.
'''

from airflow import utils
from airflow import DAG

from airflow import settings


from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.operators import EnsureDirectoryExistsOperator, RsyncOperator, EnsureConfigurationExistsSensor

from airflow.exceptions import AirflowException, AirflowSkipException

from pathlib import Path
import re

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
    'source_includes': [ 'FoilHole_*_Data_*.jpg', 'FoilHole_*_Data_*.xml', 'FoilHole_*_Data_*.mrc', 'FoilHole_*_Data_*.dm4' ],
    'destination_directory': '/gpfs/slac/cryo/fs1/exp/',
    'remove_files_after': 180, # minutes
    'remove_files_larger_than': '+100M',
}


def trigger_preprocessing(context):
    """ calls the preprocessing dag: pass the filenames of the stuff to process """
    # we have a jpg, xml, small mrc and large mrc, and gainref dm4 file
    # assume the common filename is the same and allow the  preprocessing dag wait for the other files? what happens if two separate calls to the same dag occur?
    
    found = {}
    for f in context['ti'].xcom_pull( task_ids='rsync_data', key='return_value' ):
        this = Path(f).resolve().stem
        for pattern in ( r'\-\d+$', r'\-gain\-ref$' ):
            if re.search( pattern, this):
                this = re.sub( pattern, '', this)
            # LOG.warn("mapped: %s -> %s" % (f, this))
        # only care about the xml file for now (let the dag deal with the other files
        if f.endswith('.xml'):
            LOG.warn("found EPU metadata %s" % this )
            found[this] = True

    for name,_ in found.items():
        dro = DagRunOrder(run_id='trig__%s' % datetime.utcnow().isoformat()) 
        dro.payload = { 
            'directory': context['ti'].xcom_pull( task_ids='parse_config', key='experimental_directory'),
            'base': [ i for i in found.items() ]
        }
        yield dro
    return
    
    
class TriggerMultipleDagRunOperator(TriggerDagRunOperator):
    def execute(self, context):
        count = 0
        for dro in self.python_callable(context):
            if dro:
                with create_session() as session:
                    dbag = DagBag(settings.DAGS_FOLDER)
                    trigger_dag = dbag.get_dag(self.trigger_dag_id)
                    dr = trigger_dag.create_dagrun(
                        run_id=dro.run_id,
                        state=State.RUNNING,
                        conf=dro.payload,
                        external_trigger=True)
                    self.log.info("Creating DagRun %s", dr)
                    session.add(dr)
                    session.commit() 
                    count = count + 1
            else:
                self.log.info("Criteria not met, moving on")
        if count == 0:
            raise AirflowSkipException('No external dags triggered')

with DAG( 'cryoem_krios1_file-drop',
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
        bash_command = "timeout 5 mkdir -p {{ ti.xcom_pull(task_ids='parse_config',key='experiment_directory') }}"
    )

    ###
    # rsync the globbed files over and store on target without hierachy
    ###
    t_rsync = RsyncOperator( task_id='rsync_data',
        # dry_run=True,
        source=args['source_directory'],
        target="{{ ti.xcom_pull(task_ids='parse_config',key='experiment_directory') }}",
        includes=args['source_includes'],
        prune_empty_dirs=True,
        flatten=True,
    )

    ###
    # delete files large file over a certain amount of time
    ###
    t_remove = BashOperator( task_id='remove_old_source_files',
        bash_command="find {{ params.source_directory }} {{ params.file_glob }} -type f -mmin +{{ params.age }} -size {{ params.size }} -exec rm -vf '{}' +",
        params={ 
            'source_directory': args['source_directory'],
            'file_glob': "\( -name 'FoilHole_*_Data_*.mrc' -o -name 'FoilHole_*_Data_*.dm4' \)",
            'age': args['remove_files_after'],
            'size': args['remove_files_larger_than'],
        }
    )


    ###
    # trigger another daq to handle the rest of the pipeline
    ###
    t_trigger_preprocessing = TriggerMultipleDagRunOperator( task_id='trigger_preprocessing',
        trigger_dag_id='cryoem_pre-processing',
        python_callable=trigger_preprocessing
    )
#    t_trigger_preprocessing = BashOperator( task_id='trigger_preprocessing',
#        bash_command="""
#{% for i in ti.xcom_pull(task_ids='rsync_data',key='return_value') %}
#{% if i.endswith( '.xml' ) %}
#echo airflow trigger_dag cryoem_pre-processing 
#{% endif %}
#{% endfor %}""",
#    )


    ###
    # define pipeline
    ###
    t_config >> t_exp_dir >> t_rsync >> t_remove
    # t_config >> t_rsync
    t_rsync >> t_trigger_preprocessing


