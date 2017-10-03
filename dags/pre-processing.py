from datetime import datetime
from airflow import utils
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': utils.dates.days_ago(0),
    'tem': 1,
    'experiment_id': 'abc123',
    'source_directory': '/tmp',
    'source_fileglob': '*.tif',
    'destination_directory': '/usr/local/airflow/data',
}



dag = DAG( 'cryoem_preprocessing',
    description="Conduct some initial processing to determine efficacy of CryoEM data and upload it to the elogbook",
    schedule_interval=None,
    start_date=utils.dates.days_ago(0),
    default_args=args,
    max_active_runs=1
)



def parseParameters(**kwargs):
    """read the parameters from the xml"""
    pass
t_parameters = PythonOperator(task_id='determine_tem_parameters', dag=dag,
    python_callable=parseParameters,
    op_kargs={}
)


def uploadParameters(**kwargs):
    """Push the parameter key-value pairs to the elogbook"""
    pass
t_logbook = PythonOperator(task_id='upload_parameters_to_logbook', dag=dag,
    python_callable=uploadParameters,
    op_kwargs={}
)


def tif2mrc(**kwargs):
    """Convert the tif file to mrc"""
    pass
t_tif2mrc = PythonOperator(task_id='convert_tif_to_mrc', dag=dag,
    python_callable=tif2mrc,
    op_kwargs={}
)


def motioncorr(**kwargs):
    """Run motioncorr on the data files and xcom push all of the data"""
    pass
t_motioncorr = PythonOperator(task_id='motioncorr', dag=dag,
    python_callable=motioncorr,
    op_kwargs={}
)

t_upload_motioncorr = DummyOperator(task_id='upload_motioncorr_to_logbook', dag=dag)


def ctffind(**kwargs):
    """Run ctffind on the data files and xcom push all of the data"""
    pass
t_ctffind = PythonOperator(task_id='ctffind', dag=dag,
    python_callable=ctffind,
    op_kwargs={}
)


t_upload_ctffind = DummyOperator(task_id='upload_ctffind_to_logbook', dag=dag)


t_clean = DummyOperator(task_id='clean_up', dag=dag)


t_parameters >> t_logbook >> t_clean

t_tif2mrc >> t_motioncorr >> t_upload_motioncorr >> t_clean

t_tif2mrc >> t_ctffind >> t_upload_ctffind >> t_clean

