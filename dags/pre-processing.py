
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
    parameter_files = FileSensor( task_id='parameter_files',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.xml",
        poke_interval=1,
        timeout=3,
    )
    parse_parameters = FeiEpuOperator(task_id='parse_parameters',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.xml",
    )
    # upload to the logbook
    logbook_parameters = PythonOperator(task_id='logbook_parameters',
        python_callable=uploadExperimentalParameters2Logbook,
        op_kwargs={}
    )
    influx_parameters = FeiEpu2InfluxOperator( task_id='influx_parameters',
        xcom_task_id='parse_parameters',
        xcom_key='return_value',
        host='influxdb01.slac.stanford.edu',
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    ensure_slack_channel = SlackAPIEnsureChannelOperator( task_id='ensure_slack_channel',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
    )
    # invite_slack_users = SlackAPIInviteToChannelOperator( task_id='invite_slack_users',
    invite_slack_users = NotYetImplementedOperator( task_id='invite_slack_users',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
        users=('yee',),
    )


    ###
    # get the summed jpg
    ###
    summed_preview = FileSensor( task_id='summed_preview',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.jpg",
        poke_interval=1,
        timeout=3,
    )
    # slack_summed_preview = SlackAPIUploadFileOperator( task_id='slack_summed_preview',
    #     channel="{{ dag_run.conf['experiment'][:21] }}",
    #     token=Variable.get('slack_token'),
    #     filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.jpg",
    # )

    ###
    # get the summed mrc
    ###
    summed_file = FileSensor( task_id='summed_file',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.mrc",
    )
    # TODO need parameters for input into ctffind
    ctffind_summed = LSFSubmitOperator( task_id='ctffind_summed',
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
    
    ttf_summed = LSFJobSensor( task_id='ttf_summed',
        ssh_hook=hook,
        bjobs="/afs/slac/package/lsf/curr/bin/bjobs",
        jobid="{{ ti.xcom_pull( task_ids='ctffind_summed' ) }}"
    )
    
    influx_ttf_summed = NotYetImplementedOperator( task_id='influx_ttf_summed' )
    
    
    ttf_summed_preview = FileSensor( task_id='ttf_summed_preview',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.jpg",
    )

    ttf_summed_file = FileSensor( task_id='ttf_summed_file',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.mrc",
    )


    # slack_summed_ctf = SlackAPIPostOperator( task_id='slack_summed_ctf',
    #     channel="{{ dag_run.conf['experiment'][:21] }}",
    #     token=Variable.get('slack_token'),
    #     text='ctf! ctf!'
    # )
    # slack_summed_ctf = SlackAPIUploadFileOperator( task_id='slack_summed_ctf',
    #     channel="{{ dag_run.conf['experiment'][:21] }}",
    #     token=Variable.get('slack_token'),
    #     filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.jpg",
    # )

    logbook_ttf_summed = NotYetImplementedOperator( task_id='logbook_ttf_summed' )


    summed_sidebyside = BashOperator( task_id='summed_sidebyside',
        bash_command="convert -resize 512x495 {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.jpg {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.jpg  +append {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_sidebyside.jpg"
    )

    slack_summed_sideby_side = SlackAPIUploadFileOperator( task_id='slack_summed_sideby_side',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_sidebyside.jpg",
    )


    ###
    #
    ###
    stack_file = FileGlobSensor( task_id='stack_file',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-*.mrc",
    )

    gainref_file = FileSensor( task_id='gainref_file',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.dm4",
    )

    ####
    # convert gain ref to mrc
    ####
    convert_gainref = LSFSubmitOperator( task_id='convert_gainref',
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
    new_gainref = LSFJobSensor( task_id='new_gainref',
        ssh_hook=hook,
        bjobs="/afs/slac/package/lsf/curr/bin/bjobs",
        jobid="{{ ti.xcom_pull( task_ids='convert_gain_ref' ) }}"
    )

    influx_new_gainref = LSFJob2InfluxOperator( task_id='influx_new_gainref',
        job_name='convert_gainref',
        xcom_task_id='new_gainref',
        host='influxdb01.slac.stanford.edu',
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    new_gainref_file = FileSensor( task_id='new_gainref_file',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.mrc",
        poke_interval=3,
        timeout=60,
    )


    ###
    # align the frame
    ###
    motioncorr_stack = LSFSubmitOperator( task_id='motioncorr_stack',
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
MotionCor2  -InMrc {{ ti.xcom_pull( task_ids='stack_file' ).pop(0) }} -OutMrc {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.mrc -LogFile {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.log }} -Gain {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.mrc }} -kV {{ params.kv }} -FmDose {{ params.fmdose }} -Bft {{ params.bft }} -PixSize {{ params.pixel_size }} -OutStack 1  -Gpu {{ params.gpu }}

###
# generate a preview - TODO: need single?
###
module load imod-4.9.4-intel-17.0.2-fdpbjp4
mrc2tif -j {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.mrc {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.jpg
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

    aligned_stack = LSFJobSensor( task_id='aligned_stack',
        ssh_hook=hook,
        bjobs="/afs/slac/package/lsf/curr/bin/bjobs",
        jobid="{{ ti.xcom_pull( task_ids='motioncorr_stack' ) }}",
        timeout=300,
    )

    influx_aligned_stack = LSFJob2InfluxOperator( task_id='influx_aligned_stack',
        job_name='align_stack',
        xcom_task_id='aligned_stack',
        host='influxdb01.slac.stanford.edu',
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    logbook_aligned_stack = NotYetImplementedOperator(task_id='logbook_aligned_stack')


    aligned_stack_file = FileSensor( task_id='aligned_stack_file',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_stack.mrc }}",
        poke_interval=5,
        timeout=600,
    )

    aligned_stack_preview = FileSensor( task_id='aligned_stack_preview',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.jpg }}",
        poke_interval=5,
        timeout=600,
    )

    ctffind_stack = LSFSubmitOperator( task_id='ctffind_stack',
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

    ttf_stack = LSFJobSensor( task_id='ttf_stack',
        ssh_hook=hook,
        bsub='/afs/slac/package/lsf/curr/bin/bjobs',
        jobid="{{ ti.xcom_pull( task_ids='ctffind_stack' ) }}"
    )
    
    influx_ttf_stack = LSFJob2InfluxOperator( task_id='influx_ttf_stack',
        job_name='ttf_stack',
        xcom_task_id='ttf_stack',
        host='influxdb01.slac.stanford.edu',
        experiment="{{ dag_run.conf['experiment'] }}",
    )
    
    aligned_ttf_file = FileSensor( task_id='aligned_ttf_file',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_ctf.mrc }}",
        poke_interval=3,
        timeout=60,
    )
    
    aligned_ttf_file_preview = FileSensor( task_id='aligned_ttf_file_preview',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_ctf.jpg }}",
        poke_interval=3,
        timeout=60,
    )
    
    aligned_sidebyside = BashOperator( task_id='aligned_sidebyside',
        bash_command="convert -resize 512x495 {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.jpg {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_ctf.jpg  +append {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_sidebyside.jpg"
    )
    
    slack_aligned_sidebyside = SlackAPIUploadFileOperator( task_id='slack_aligned_sidebyside',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned_sidebyside.jpg",
    )
    
    logbook_ttf_stack = NotYetImplementedOperator(task_id='logbook_ttf_stack')



    ###
    # define pipeline
    ###

    parameter_files >> parse_parameters >> logbook_parameters 
    summed_preview  >> logbook_parameters
    # summed_preview >> slack_summed_preview
    parse_parameters >> influx_parameters 

    parse_parameters >> ctffind_summed >> ttf_summed
    ctffind_summed >> influx_ttf_summed

    ensure_slack_channel >> invite_slack_users
    # ensure_slack_channel >> slack_summed_preview
    # ensure_slack_channel >> slack_summed_ctf
    ensure_slack_channel >> slack_summed_sideby_side
    
    summed_preview >> summed_sidebyside
    ttf_summed_preview >> summed_sidebyside
    summed_sidebyside >> slack_summed_sideby_side
    

    summed_file >> ctffind_summed
    ttf_summed >> logbook_ttf_summed 
    ttf_summed >> ttf_summed_preview # >> slack_summed_ctf
    ttf_summed >> ttf_summed_file
    
    stack_file >> motioncorr_stack
    gainref_file >> convert_gainref >> new_gainref >> motioncorr_stack
    new_gainref >> influx_new_gainref
    new_gainref >> new_gainref_file >> motioncorr_stack
    motioncorr_stack >> aligned_stack 
    aligned_stack >> influx_aligned_stack

    aligned_stack >> ctffind_stack
    ctffind_stack >> aligned_ttf_file
    ctffind_stack >> aligned_ttf_file_preview
    
    aligned_stack >> logbook_aligned_stack 

    aligned_stack >> aligned_stack_file >> ctffind_stack >> ttf_stack >> logbook_ttf_stack 
    aligned_stack >> aligned_stack_preview
    
    aligned_stack_preview >> aligned_sidebyside
    aligned_ttf_file_preview >> aligned_sidebyside
    aligned_sidebyside >> slack_aligned_sidebyside
    ensure_slack_channel >> slack_aligned_sidebyside
    
    ttf_stack >> influx_ttf_stack
