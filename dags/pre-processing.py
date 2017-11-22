
from airflow import DAG

from airflow.models import Variable

from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import BaseSensorOperator

from airflow.operators import FileSensor, FileGlobSensor
from airflow.operators import LSFSubmitOperator, LSFJobSensor
from airflow.contrib.hooks import SSHHook

from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators import SlackAPIEnsureChannelOperator, SlackAPIInviteToChannelOperator, SlackAPIUploadFileOperator
from airflow.operators import FeiEpuOperator, FeiEpu2InfluxOperator, LSFJob2InfluxOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from datetime import datetime

# import influxdb
from pprint import pprint, pformat


import logging
LOG = logging.getLogger(__name__)

args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': datetime( 2017,1,1 ),
    'ssh_connection_id': 'ssh_docker_host',
}



def uploadExperimentalParameters2Logbook(ds, **kwargs):
    """Push the parameter key-value pairs to the elogbook"""
    data = kwargs['ti'].xcom_pull( task_ids='parse_parameters' )
    LOG.warn("data: %s" % (data,))
    raise AirflowSkipException('not yet implemented')


class NotYetImplementedOperator(DummyOperator):
    ui_color = '#d3d3d3'


###
# define the workflow
###
with DAG( 'cryoem_pre-processing',
        description="Conduct some initial processing to determine efficacy of CryoEM data and upload it to the elogbook",
        schedule_interval=None,
        default_args=args,
        catchup=False,
        max_active_runs=5
    ) as dag:

    # hook to container host for lsf commands
    hook = SSHHook(conn_id=args['ssh_connection_id'])

    ###
    # parse the epu xml metadata file
    ###
    t_wait_params = FileSensor( task_id='wait_for_parameters',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.xml",
        poke_interval=1,
        timeout=3,
    )
    t_parameters = FeiEpuOperator(task_id='parse_parameters',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.xml",
    )
    # upload to the logbook
    t_param_logbook = PythonOperator(task_id='upload_parameters_to_logbook',
        python_callable=uploadExperimentalParameters2Logbook,
        op_kwargs={}
    )
    t_param_influx = FeiEpu2InfluxOperator( task_id='upload_parameters_to_influx',
        xcom_task_id='parse_parameters',
        xcom_key='return_value',
        host='influxdb01.slac.stanford.edu',
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    t_ensure_slack_channel = SlackAPIEnsureChannelOperator( task_id='ensure_slack_channel',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
    )
    # t_invite_to_slack_channel = SlackAPIInviteToChannelOperator( task_id='invite_slack_users',
    t_invite_to_slack_channel = NotYetImplementedOperator( task_id='invite_slack_users',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
        users=('yee',),
    )


    ###
    # get the summed jpg
    ###
    t_wait_preview = FileSensor( task_id='wait_for_preview',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.jpg",
        poke_interval=1,
        timeout=3,
    )
    t_slack_preview = SlackAPIUploadFileOperator( task_id='slack_preview',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.jpg",
    )    

    ###
    # get the summed mrc
    ###
    t_wait_summed = FileSensor( task_id='wait_for_summed',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.mrc",
    )
    # TODO need parameters for input into ctffind
    t_ctf_summed = LSFSubmitOperator( task_id='ctf_summed',
        ssh_hook=hook,
        queue_name="ocio-gpu",
        bsub='/afs/slac/package/lsf/curr/bin/bsub',
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.job
###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# calculate fft
###
module load ctffind4-4.1.8-intel-17.0.2-gfcjad5
cd {{ dag_run.conf['directory'] }}
ctffind > {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.log <<-'__CTFFIND_EOF__'
{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.mrc
{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.mrc
{{ params.pixel_size }}
{{ params.kv }}
{{ params.cs }}
0.07
512
30
5
5000
50000
500
no
no
yes
100
no
no
__CTFFIND_EOF__

###
# convert fft to jpg for preview
###
module load eman2-master-gcc-4.8.5-pri5spm
e2proc2d.py {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.mrc {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.jpg
        """,
        params={
            'kv': 300,
            'pixel_size': 1.246,
            'cs': 2.7,
        }
    )
    
    t_wait_ctf_summed = LSFJobSensor( task_id='wait_ctf_summed',
        ssh_hook=hook,
        bjobs="/afs/slac/package/lsf/curr/bin/bjobs",
        jobid="{{ ti.xcom_pull( task_ids='ctf_summed' ) }}"
    )
    
    t_influx_ctf_summed = NotYetImplementedOperator( task_id='influx_ctf_summed' )
    
    
    t_ctf_summed_preview = FileSensor( task_id='ctf_summed_preview',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.jpg",
    )
    # t_slack_summed_ctf = SlackAPIPostOperator( task_id='slack_summed_ctf',
    #     channel="{{ dag_run.conf['experiment'][:21] }}",
    #     token=Variable.get('slack_token'),
    #     text='ctf! ctf!'
    # )
    t_slack_summed_ctf = SlackAPIUploadFileOperator( task_id='slack_summed_ctf',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.jpg",
    )    



    t_ctf_summed_logbook = NotYetImplementedOperator( task_id='upload_summed_ctf_logbook' )


    t_summed_sidebyside = NotYetImplementedOperator( task_id='summed_sidebyside',
        # take the jpg and the ctf jpg and put it togehter to upload
    )

    t_slack_summed_sidebyside = NotYetImplementedOperator( task_id='slack_summed_sideby_side',
        # upload side by side image to slack
    )


    ###
    #
    ###
    t_wait_stack = FileGlobSensor( task_id='wait_for_stack',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-*.mrc",
    )

    t_wait_gainref = FileSensor( task_id='wait_for_gainref',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.dm4",
    )

    ####
    # convert gain ref to mrc
    ####
    t_gain_ref = LSFSubmitOperator( task_id='convert_gain_ref',
        ssh_hook=hook,
        queue_name="ocio-gpu",
        bsub='/afs/slac/package/lsf/curr/bin/bsub',
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.job
###
# bootstrap
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# convert using imod
###
module load imod-4.9.4-intel-17.0.2-fdpbjp4
tif2mrc {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.dm4 {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.mrc

""",
    )
    t_wait_job_new_gainref = LSFJobSensor( task_id='wait_new_gainref',
        ssh_hook=hook,
        bjobs="/afs/slac/package/lsf/curr/bin/bjobs",
        jobid="{{ ti.xcom_pull( task_ids='convert_gain_ref' ) }}"
    )

    t_influx_new_gainref = LSFJob2InfluxOperator( task_id='influx_new_gainref',
        job_name='convert_gainref',
        xcom_task_id='wait_new_gainref',
        host='influxdb01.slac.stanford.edu',
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    t_wait_new_gainref = FileSensor( task_id='wait_for_new_gainref',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.mrc",
        poke_interval=3,
        timeout=60,
    )


    ###
    # align the frame
    ###
    t_motioncorr_stack = LSFSubmitOperator( task_id='motioncorr_stack',
        ssh_hook=hook,
        queue_name="ocio-gpu",
        bsub='/afs/slac/package/lsf/curr/bin/bsub',
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
# #BSUB -R "select[ngpus=1]"
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.job

###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# align the frames
###  
module load motioncor2-1.0.2-gcc-4.8.5-lrpqluf
MotionCor2  -InMrc {{ ti.xcom_pull( task_ids='wait_for_stack' ).pop(0) }} -OutMrc {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.mrc -LogFile {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.log }} -Gain {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.mrc }} -kV {{ params.kv }} -FmDose {{ params.fmdose }} -Bft {{ params.bft }} -PixSize {{ params.pixel_size }} -OutStack 1  -Gpu {{ params.gpu }}
        """,
        params={
            'kv': 300,
            'fmdose': 2,
            'bft': 150,
            'pixel_size': 1.246,
            'patch': '5 5',
            'gpu': 0,
        },
        # -Patch {{ params.patch }}
    )

    t_wait_motioncorr_stack = LSFJobSensor( task_id='wait_motioncor_stack',
        ssh_hook=hook,
        bjobs="/afs/slac/package/lsf/curr/bin/bjobs",
        jobid="{{ ti.xcom_pull( task_ids='motioncorr_stack' ) }}",
        timeout=300,
    )

    t_influx_motioncorr_stack = LSFJob2InfluxOperator( task_id='influx_motioncorr_stack',
        job_name='align_stack',
        xcom_task_id='wait_motioncor_stack',
        host='influxdb01.slac.stanford.edu',
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    t_motioncorr_2_logbbok = NotYetImplementedOperator(task_id='upload_motioncorr_to_logbook')


    t_wait_for_aligned = FileSensor( task_id='wait_for_aligned',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_stack.mrc }}",
        poke_interval=5,
        timeout=600,
    )



    t_ctffind_stack = LSFSubmitOperator( task_id='ctffind_stack',
        ssh_hook=hook,
        queue_name="ocio-gpu",
        bsub='/afs/slac/package/lsf/curr/bin/bsub',
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_ctf.job
###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# calculate fft
###
module load ctffind4-4.1.8-intel-17.0.2-gfcjad5
cd {{ dag_run.conf['directory'] }}
ctffind > {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_ctf.log <<-'__CTFFIND_EOF__'
{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.mrc
{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_ctf.mrc
{{ params.pixel_size }}
{{ params.kv }}
{{ params.cs }}
0.07
512
30
5
5000
50000
500
no
no
yes
100
no
no
__CTFFIND_EOF__

###
# convert fft to jpg for preview
###
module load eman2-master-gcc-4.8.5-pri5spm
e2proc2d.py {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_ctf.mrc {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_ctf.jpg
""",
        params={
            'kv': 300,
            'pixel_size': 1.246,
            'cs': 2.7,
        }
    )

    t_wait_ctffind_stack = LSFJobSensor( task_id='wait_ctffind_stack',
        ssh_hook=hook,
        bsub='/afs/slac/package/lsf/curr/bin/bjobs',
        jobid="{{ ti.xcom_pull( task_ids='ctffind_stack' ) }}"
    )
    
    t_influx_ctffind_stack = LSFJob2InfluxOperator( task_id='influx_ctffind_stack',
        job_name='ctf_stack',
        xcom_task_id='wait_ctffind_stack',
        host='influxdb01.slac.stanford.edu',
        experiment="{{ dag_run.conf['experiment'] }}",
    )
    
    t_ctffind_stack_logbook = NotYetImplementedOperator(task_id='upload_ctffind_to_logbook')



    ###
    # define pipeline
    ###

    t_wait_params >> t_parameters >> t_param_logbook 
    t_wait_preview  >> t_param_logbook
    t_wait_preview >> t_slack_preview
    t_parameters >> t_param_influx 

    t_parameters >> t_ctf_summed >> t_wait_ctf_summed
    t_ctf_summed >> t_influx_ctf_summed

    t_ensure_slack_channel >> t_invite_to_slack_channel
    t_ensure_slack_channel >> t_slack_preview
    t_ensure_slack_channel >> t_slack_summed_ctf
    
    t_wait_preview >> t_summed_sidebyside
    t_ctf_summed_preview >> t_summed_sidebyside
    t_summed_sidebyside >> t_slack_summed_sidebyside
    

    t_wait_summed >> t_ctf_summed  
    t_wait_ctf_summed >> t_ctf_summed_logbook 
    t_wait_ctf_summed >> t_ctf_summed_preview >> t_slack_summed_ctf

    t_wait_stack >> t_motioncorr_stack
    t_wait_gainref >> t_gain_ref >> t_wait_job_new_gainref >> t_motioncorr_stack
    t_wait_job_new_gainref >> t_influx_new_gainref
    t_wait_job_new_gainref >> t_wait_new_gainref >> t_motioncorr_stack
    t_motioncorr_stack >> t_wait_motioncorr_stack 
    t_wait_motioncorr_stack >> t_influx_motioncorr_stack

    t_wait_motioncorr_stack >> t_ctffind_stack
    t_wait_motioncorr_stack >> t_motioncorr_2_logbbok 

    t_wait_motioncorr_stack >> t_wait_for_aligned >> t_ctffind_stack >> t_wait_ctffind_stack >> t_ctffind_stack_logbook 
    t_wait_ctffind_stack >> t_influx_ctffind_stack
