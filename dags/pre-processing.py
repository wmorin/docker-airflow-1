from datetime import datetime
from airflow import utils
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import FileGlobSensor 



dag = DAG( 'cryoem_preprocessing',
    description="Ingestion of CryoEM data",
    schedule_interval="* * * * *",
    start_date=utils.dates.days_ago(0)
)



t_wait = FileGlobSensor(task_id='wait_for_new_files', dag=dag,
    dirpath='/tmp',
    pattern='*.tif')
t_stage = DummyOperator(task_id='rename_new_files_for_staging', dag=dag)
t_parameters = DummyOperator(task_id='determine_tem_parameters', dag=dag)
t_logbook = DummyOperator(task_id='upload_parameters_to_logbook', dag=dag)
t_move = DummyOperator(task_id='move_data_file', dag=dag)
t_motioncorr = DummyOperator(task_id='motioncorr_data_file', dag=dag)
t_tif2mrc = DummyOperator(task_id='convert_tif_to_mrc', dag=dag)
t_ctffind = DummyOperator(task_id='ctffind_data', dag=dag)
t_clean = DummyOperator(task_id='remove_new_files', dag=dag)


t_wait.set_downstream(t_stage)
t_stage.set_downstream(t_parameters)
t_parameters.set_downstream(t_logbook)
t_stage.set_downstream(t_move)
t_move.set_downstream(t_tif2mrc)
t_tif2mrc.set_downstream(t_motioncorr)
t_tif2mrc.set_downstream(t_ctffind)
t_clean.set_upstream(t_motioncorr)
t_clean.set_upstream(t_ctffind)
t_clean.set_upstream(t_logbook)
