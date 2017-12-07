
from airflow import DAG

from airflow.models import Variable

from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import BaseSensorOperator

from airflow.contrib.hooks import SSHHook

from airflow.operators import FileSensor, FileGlobSensor
from airflow.operators import LSFSubmitOperator, LSFJobSensor, LSFOperator

from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators import SlackAPIEnsureChannelOperator, SlackAPIInviteToChannelOperator, SlackAPIUploadFileOperator
from airflow.operators import Ctffind4DataSensor

from airflow.operators import FeiEpuOperator
from airflow.operators import FeiEpu2InfluxOperator, LSFJob2InfluxOperator, GenericInfluxOperator

from airflow.exceptions import AirflowException, AirflowSkipException, AirflowSensorTimeout

from datetime import datetime, timedelta


import logging
LOG = logging.getLogger(__name__)

args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': datetime( 2017,1,1 ),
    'ssh_connection_id': 'ssh_docker_host',
    'influx_host': 'influxdb01.slac.stanford.edu',
    'kv': 300,
    'pixel_size': 1.08,
    'cs': 2.7,
    'gain_referenced': True, # if the mrcs are already gain refed
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
with DAG( '20171204_sroh-hsp_krios1',
        description="Pre-processing of TEM1 CryoEM data",
        schedule_interval=None,
        default_args=args,
        catchup=False,
        max_active_runs=8,
        concurrency=32,
        dagrun_timeout=3600,
    ) as dag:

    # hook to container host for lsf commands
    hook = SSHHook(conn_id=args['ssh_connection_id'])
    # lsftest_hook = SSHHook(conn_id='ssh_lsf_test')
    
    ###
    # parse the epu xml metadata file
    ###
    parameter_file = FileGlobSensor( task_id='parameter_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}.xml",
        recursive=True,
        poke_interval=1,
    )
    parse_parameters = FeiEpuOperator(task_id='parse_parameters',
        filepath="{{ ti.xcom_pull( task_ids='parameter_file' )[0] }}",
    )
    # upload to the logbook
    logbook_parameters = PythonOperator(task_id='logbook_parameters',
        python_callable=uploadExperimentalParameters2Logbook,
        op_kwargs={}
    )
    influx_parameters = FeiEpu2InfluxOperator( task_id='influx_parameters',
        xcom_task_id='parse_parameters',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    ensure_slack_channel = SlackAPIEnsureChannelOperator( task_id='ensure_slack_channel',
        channel="{{ dag_run.conf['experiment'][:21] | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
    )
    # invite_slack_users = SlackAPIInviteToChannelOperator( task_id='invite_slack_users',
    invite_slack_users = NotYetImplementedOperator( task_id='invite_slack_users',
        # channel="{{ dag_run.conf['experiment'][:21] }}",
        # token=Variable.get('slack_token'),
        # users=('yee',),
    )


    ###
    # get the summed jpg
    ###
    summed_preview = FileGlobSensor( task_id='summed_preview',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}.jpg",
        recursive=True,
        poke_interval=1,
    )

    ###
    # get the summed mrc
    ###
    summed_file = FileGlobSensor( task_id='summed_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}.mrc",
        recursive=True,
    )
    # TODO need parameters for input into ctffind
    ctffind_summed = LSFSubmitOperator( task_id='ctffind_summed',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/summed/ctffind4/4.1.8/{{ dag_run.conf['base'] }}_ctf.job
#BSUB -W 3
#BSUB -We 1
#BSUB -n 1

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
mkdir -p {{ dag_run.conf['directory'] }}/summed/ctffind4/4.1.8/
cd {{ dag_run.conf['directory'] }}/summed/ctffind4/4.1.8/
ctffind > {{ dag_run.conf['base'] }}_ctf.log <<-'__CTFFIND_EOF__'
{{ ti.xcom_pull( task_ids='summed_file' )[0] }}
{{ dag_run.conf['base'] }}_ctf.mrc
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
""",
        params={
            'kv': args['kv'],
            'pixel_size': args['pixel_size'],
            'cs': args['cs'],
        }
    )

    convert_summed_ttf_preview = LSFOperator( task_id='convert_summed_ttf_preview',
        ssh_hook=hook,
        poke_interval=1,
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/summed/ctffind4/4.1.8/{{ dag_run.conf['base'] }}_ctf_preview.job
#BSUB -w "done({{ ti.xcom_pull( task_ids='ctffind_summed' )['jobid'] }})"
#BSUB -W 3
#BSUB -We 1
#BSUB -n 1

###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# convert fft to jpg for preview
###
module load eman2-master-gcc-4.8.5-pri5spm
export PYTHON_EGG_CACHE='/tmp'
cd {{ dag_run.conf['directory'] }}/summed/ctffind4/4.1.8/
e2proc2d.py \
    {{ dag_run.conf['base'] }}_ctf.mrc \
    {{ dag_run.conf['base'] }}_ctf.jpg
""",
    )

    influx_summed_preview = LSFJob2InfluxOperator( task_id='influx_summed_preview',
        job_name='summed_preview',
        xcom_task_id='convert_summed_ttf_preview',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )
    
    ttf_summed = LSFJobSensor( task_id='ttf_summed',
        ssh_hook=hook,
        jobid="{{ ti.xcom_pull( task_ids='ctffind_summed' )['jobid'] }}",
        retries=2,
        retry_delay=timedelta(seconds=1),
        poke_interval=1,
    )
    
    influx_summed_ttf = LSFJob2InfluxOperator( task_id='influx_summed_ttf',
        job_name='ttf_summed',
        xcom_task_id='ttf_summed',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )
    
    summed_ttf_preview = FileGlobSensor( task_id='summed_ttf_preview',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_ctf.jpg",
        recursive=True,
        poke_interval=1,
    )

    summed_ttf_file = FileGlobSensor( task_id='summed_ttf_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_ctf.mrc",
        recursive=True,
        poke_interval=1,
    )

    summed_ttf_data = Ctffind4DataSensor( task_id='summed_ttf_data',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_ctf.txt",
        recursive=True,
        poke_interval=1,
    )

    influx_summed_ttf_data = GenericInfluxOperator( task_id='influx_summed_ttf_data',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='summed_file' )[0] }}",
        tags={
            'app': 'ctffind',
            'version': '4.1.8',
            'state': 'unaligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        tags2="{{ ti.xcom_pull( task_ids='summed_ttf_data', key='context' ) }}",
        fields="{{ ti.xcom_pull( task_ids='summed_ttf_data' ) }}",
    )

    logbook_summed_ttf = NotYetImplementedOperator( task_id='logbook_summed_ttf' )


    ###
    #
    ###
    stack_file = FileGlobSensor( task_id='stack_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}-*.mrc",
        recursive=True,
        poke_interval=1,
    )


    if not args['gain_referenced']:
        gainref_file = FileGlobSensor( task_id='gainref_file',
            filepath="{{ dag_run.conf['directory'] }}/**{{ dag_run.conf['base'] }}-gain-ref.dm4",
            recursive=True,
            poke_interval=1,
        )

        ####
        # convert gain ref to mrc
        ####
        convert_gainref = LSFSubmitOperator( task_id='convert_gainref',
            ssh_hook=hook,
            env={
                'LSB_JOB_REPORT_MAIL': 'N',
            },
            lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.job
#BSUB -W 3
#BSUB -We 1
#BSUB -n 1
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
cd {{ dag_run.conf['directory'] }}
tif2mrc \
    {{ dag_run.conf['base'] }}-gain-ref.dm4 \
    {{ dag_run.conf['base'] }}-gain-ref.mrc
                """,
        )

        new_gainref = LSFJobSensor( task_id='new_gainref',
            ssh_hook=hook,
            jobid="{{ ti.xcom_pull( task_ids='convert_gainref' )['jobid'] }}",
            poke_interval=1,
        )

        influx_new_gainref = LSFJob2InfluxOperator( task_id='influx_new_gainref',
            job_name='convert_gainref',
            xcom_task_id='new_gainref',
            host=args['influx_host'],
            experiment="{{ dag_run.conf['experiment'] }}",
        )

        new_gainref_file = FileGlobSensor( task_id='new_gainref_file',
            filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}-gain-ref.mrc",
            recursive=True,
            poke_interval=1,
        )

    ###
    # align the frame
    ###
    motioncorr_stack = LSFSubmitOperator( task_id='motioncorr_stack',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        retries=2,
        retry_delay=timedelta(seconds=1),
#BSUB -R "select[ngpus>0] rusage[ngpus_shared=1]"
        lsf_script="""
#BSUB -R "select[ngpus>0] rusage[ngpus_excl_p=1]"
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/{{ dag_run.conf['base'] }}_aligned.job
#BSUB -W 10
#BSUB -We 1
#BSUB -n 1
###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# debug lsf issues
###
module load cuda-8.0.61-gcc-4.8.5-4boigic
/afs/slac/package/spack/opt/spack/linux-rhel7-x86_64/gcc-4.8.5/cuda-8.0.61-4boigicgv475rgoirhsdyiwsdz5rgde7/samples/1_Utilities/deviceQueryDrv/deviceQueryDrv

###
# align the frames
###  
module load motioncor2-1.0.2-gcc-4.8.5-lrpqluf
mkdir -p {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/
MotionCor2  \
    -InMrc {{ ti.xcom_pull( task_ids='stack_file' )[0] }} \
    -OutMrc {{ dag_run.conf['base'] }}_aligned.mrc \
    -LogFile {{ dag_run.conf['base'] }}_aligned.log \
    -kV {{ params.kv }} \
    -FmDose {{ dag_run.conf['fmdose'] }} \
    -Bft {{ params.bft }} \
    -PixSize {{ params.pixel_size }} \
    -Patch {{ params.patch }} \
    -Iter {{ params.iter }} \
    -OutStack 1 \
    -Gpu {{ params.gpu }}
""",
        params={
            'kv': args['kv'],
            'bft': 150,
            'pixel_size': args['pixel_size'],
            'patch': '3 3',
            'iter': 10,
            'gpu': 0,
        },
    )
    #BSUB -w "done({{ ti.xcom_pull( task_ids='convert_gainref' )['jobid'] }})"
    # -Gain {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.mrc \

    align = LSFJobSensor( task_id='align',
        ssh_hook=hook,
        jobid="{{ ti.xcom_pull( task_ids='motioncorr_stack' )['jobid'] }}",
        poke_interval=5,
    )

    influx_aligned = LSFJob2InfluxOperator( task_id='influx_aligned',
        job_name='align_stack',
        xcom_task_id='align',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    aligned_stack_file = FileGlobSensor( task_id='aligned_stack_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned_Stk.mrc",
        recursive=True,
        poke_interval=5,
    )


    convert_aligned_preview = LSFOperator( task_id='convert_aligned_preview',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/{{ dag_run.conf['base'] }}_aligned_preview.job
#BSUB -w "done({{ ti.xcom_pull( task_ids='motioncorr_stack' )['jobid'] }})"
#BSUB -W 10
#BSUB -We 2
#BSUB -n 1
###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# generate a preview
###
module load eman2-master-gcc-4.8.5-pri5spm
export PYTHON_EGG_CACHE='/tmp'
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/
e2proc2d.py \
    {{ dag_run.conf['base'] }}_aligned.mrc \
    {{ dag_run.conf['base'] }}_aligned.jpg \
    --process filter.lowpass.gauss:cutoff_freq=0.05
""",
        poke_interval=1,
    )


    influx_aligned_preview = LSFJob2InfluxOperator( task_id='influx_aligned_preview',
        job_name='aligned_preview',
        xcom_task_id='convert_aligned_preview',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )


    logbook_aligned = NotYetImplementedOperator(task_id='logbook_aligned')


    aligned_file = FileGlobSensor( task_id='aligned_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned.mrc",
        recursive=True,
        poke_interval=1,
    )

    aligned_preview = FileGlobSensor( task_id='aligned_preview',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned.jpg",
        recursive=True,
        poke_interval=1,
    )

    ctffind_aligned = LSFSubmitOperator( task_id='ctffind_aligned',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/ctffind4/4.1.8/{{ dag_run.conf['base'] }}_aligned_ctf.job
#BSUB -w "done({{ ti.xcom_pull( task_ids='motioncorr_stack' )['jobid'] }})"
#BSUB -W 3
#BSUB -We 1
#BSUB -n 1
###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# calculate fft
# beware we do not use aligned_file's xcom as it would not have completed yet
###
module load ctffind4-4.1.8-intel-17.0.2-gfcjad5
mkdir -p {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/ctffind4/4.1.8/
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/ctffind4/4.1.8/
ctffind > {{ dag_run.conf['base'] }}_aligned_ctf.log <<-'__CTFFIND_EOF__'
{{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/{{ dag_run.conf['base'] }}_aligned.mrc
{{ dag_run.conf['base'] }}_aligned_ctf.mrc
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
""",
        params={
            'kv': args['kv'],
            'pixel_size': args['pixel_size'],
            'cs': args['cs'],
        }
    )

    convert_aligned_ttf_preview = LSFOperator( task_id='convert_aligned_ttf_preview',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        poke_interval=1,
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/ctffind4/4.1.8/{{ dag_run.conf['base'] }}_aligned_ctf.job
#BSUB -w "done({{ ti.xcom_pull( task_ids='ctffind_aligned' )['jobid'] }})"
#BSUB -W 3
#BSUB -We 1
#BSUB -n 1

###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# convert fft to jpg for preview
###
module load eman2-master-gcc-4.8.5-pri5spm
export PYTHON_EGG_CACHE='/tmp'
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.2/ctffind4/4.1.8/
e2proc2d.py \
    {{ dag_run.conf['base'] }}_aligned_ctf.mrc \
    {{ dag_run.conf['base'] }}_aligned_ctf.jpg
""",
    )

    influx_ttf_preview = LSFJob2InfluxOperator( task_id='influx_ttf_preview',
        job_name='ttf_preview',
        xcom_task_id='convert_aligned_ttf_preview',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )


    ttf_aligned = LSFJobSensor( task_id='ttf_aligned',
        ssh_hook=hook,
        jobid="{{ ti.xcom_pull( task_ids='ctffind_aligned' )['jobid'] }}",
        retries=2,
        retry_delay=timedelta(seconds=1),
        poke_interval=1,
    )
    
    influx_ttf_aligned = LSFJob2InfluxOperator( task_id='influx_ttf_aligned',
        job_name='ttf_aligned',
        xcom_task_id='ttf_aligned',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )
    
    aligned_ttf_file = FileGlobSensor( task_id='aligned_ttf_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned_ctf.mrc",
        recursive=True,
        poke_interval=1,
    )
    
    aligned_ttf_preview = FileGlobSensor( task_id='aligned_ttf_preview',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned_ctf.jpg",
        recursive=1,
        poke_interval=1,
    )
    
    aligned_ttf_data = Ctffind4DataSensor( task_id='aligned_ttf_data',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned_ctf.txt",
        recursive=True,
    )

    influx_aligned_ttf_data = GenericInfluxOperator( task_id='influx_aligned_ttf_data',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='stack_file' )[0] }}",
        tags={
            'app': 'ctffind',
            'version': '4.1.8',
            'state': 'aligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        tags2="{{ ti.xcom_pull( task_ids='aligned_ttf_data', key='context' ) }}",
        fields="{{ ti.xcom_pull( task_ids='aligned_ttf_data' ) }}",
    )

    previews = BashOperator( task_id='previews',
        bash_command="""
            # summed preview
            mkdir -p {{ dag_run.conf['directory'] }}/summed/previews
            cd {{ dag_run.conf['directory'] }}/summed/previews/
            convert \
                -resize 512x495 \
                {{ ti.xcom_pull( task_ids='summed_preview' )[0] }} \
                {{ ti.xcom_pull( task_ids='summed_ttf_preview' )[0] }} \
                +append -pointsize 36 -fill yellow -draw 'text 880,478 \"{{ '%0.3f' | format(ti.xcom_pull( task_ids='summed_ttf_data' )['resolution']) }}Å\"' \
                {{ dag_run.conf['base'] }}_sidebyside.jpg

            # aligned preview
            mkdir -p {{ dag_run.conf['directory'] }}/aligned/previews/
            cd {{ dag_run.conf['directory'] }}/aligned/previews/
            convert \
                -resize 512x495 \
                {{ ti.xcom_pull( task_ids='aligned_preview' )[0] }} \
                {{ ti.xcom_pull( task_ids='aligned_ttf_preview' )[0] }} \
                +append  \
                -pointsize 36 -fill orange -draw 'text 880,478 \"{{ '%0.3f' | format(ti.xcom_pull( task_ids='aligned_ttf_data' )['resolution']) }}Å\"' \
                {{ dag_run.conf['base'] }}_aligned_sidebyside.jpg
                
            # quad preview
            mkdir -p {{ dag_run.conf['directory'] }}/previews/
            cd {{ dag_run.conf['directory'] }}/previews/
            convert \
                {{ dag_run.conf['directory'] }}/summed/previews/{{ dag_run.conf['base'] }}_sidebyside.jpg \
                {{ dag_run.conf['directory'] }}/aligned/previews/{{ dag_run.conf['base'] }}_aligned_sidebyside.jpg \
                -append \
                {{ dag_run.conf['base'] }}_full_sidebyside.jpg       
        """
    ) 
    
    slack_full_preview = SlackAPIUploadFileOperator( task_id='slack_full_preview',
        channel="{{ dag_run.conf['experiment'][:21] | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
        filepath="{{ dag_run.conf['directory'] }}/previews/{{ dag_run.conf['base'] }}_full_sidebyside.jpg",
    )
    
    logbook_ttf_aligned = NotYetImplementedOperator(task_id='logbook_ttf_aligned')



    ###
    # define pipeline
    ###

    parameter_file >> parse_parameters >> logbook_parameters 
    summed_preview  >> logbook_parameters
    parse_parameters >> influx_parameters 

    parse_parameters >> ctffind_summed >> ttf_summed
    ctffind_summed >> convert_summed_ttf_preview >> influx_summed_preview
    ttf_summed >> influx_summed_ttf

    ensure_slack_channel >> invite_slack_users
    
    summed_preview >> previews
    summed_ttf_preview >> previews

    summed_file >> ctffind_summed
    ttf_summed >> logbook_summed_ttf 
    convert_summed_ttf_preview >> summed_ttf_preview
    ttf_summed >> summed_ttf_file
    ttf_summed >> summed_ttf_data
    
    summed_ttf_data >> previews
    summed_ttf_data >> influx_summed_ttf_data
    
    stack_file >> motioncorr_stack >> convert_aligned_preview

    if not args['gain_referenced']:
        gainref_file >> convert_gainref >> motioncorr_stack
        new_gainref >> influx_new_gainref
        convert_gainref >> new_gainref
        new_gainref >> new_gainref_file

    motioncorr_stack >> align 
    align >> aligned_stack_file
    align >> influx_aligned

    ttf_aligned >> aligned_ttf_file
    convert_aligned_ttf_preview >> aligned_ttf_preview
    convert_aligned_ttf_preview >> influx_ttf_preview
    
    ttf_aligned >> aligned_ttf_data
    aligned_ttf_data >> previews
    aligned_ttf_data >> influx_aligned_ttf_data
    
    align >> logbook_aligned 

    align >> aligned_file 
    motioncorr_stack >> ctffind_aligned >> ttf_aligned >> logbook_ttf_aligned 
    ctffind_aligned >> convert_aligned_ttf_preview 
    convert_aligned_preview >> aligned_preview
    convert_aligned_preview >> influx_aligned_preview
    
    aligned_preview >> previews
    aligned_ttf_preview >> previews
    
    ensure_slack_channel >> slack_full_preview
    previews >> slack_full_preview
    
    ttf_aligned >> influx_ttf_aligned
