from airflow import DAG

from airflow.models import Variable

from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import BaseSensorOperator

from airflow.contrib.hooks import SSHHook
from airflow.hooks.http_hook import HttpHook

from airflow.operators import FileInfoSensor
from airflow.operators import LSFSubmitOperator, LSFJobSensor, LSFOperator

from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators import SlackAPIEnsureChannelOperator, SlackAPIInviteToChannelOperator, SlackAPIUploadFileOperator
from airflow.operators import Ctffind4DataSensor
from airflow.operators import MotionCor2DataSensor

from airflow.operators import FeiEpuOperator
from airflow.operators import FeiEpu2InfluxOperator, LSFJob2InfluxOperator, GenericInfluxOperator

from airflow.operators import LogbookConfigurationSensor, LogbookRegisterFileOperator, LogbookRegisterRunParamsOperator, LogbookCreateRunOperator

from airflow.exceptions import AirflowException, AirflowSkipException, AirflowSensorTimeout

import os
from datetime import datetime, timedelta


import logging
LOG = logging.getLogger(__name__)

args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': datetime( 2018,1,1 ),
    'ssh_connection_id': 'ssh_docker_host',
    'logbook_connection_id': 'cryoem_logbook',
    'influx_host': 'influxdb01.slac.stanford.edu',
    'convert_gainref':   True, #False, 
    'apply_gainref':     True, #False,
    'daq_software':      '__imaging_software__',
    'max_active_runs':   10,
    # 'apix':              1.35,
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
with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Pre-processing of CryoEM data",
        schedule_interval=None,
        default_args=args,
        catchup=False,
        max_active_runs=args['max_active_runs'],
        concurrency=72,
        dagrun_timeout=1800,
    ) as dag:

    # hook to container host for lsf commands
    hook = SSHHook(conn_id=args['ssh_connection_id'])
    logbook_hook = HttpHook( http_conn_id=args['logbook_connection_id'], method='GET' )

    ###
    # parse the epu xml metadata file
    ###
    if args['daq_software'] == 'EPU':
        parameter_file = FileInfoSensor( task_id='parameter_file',
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
        retries=2,
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
    if args['daq_software'] == 'SerialEM':
        sum = LSFOperator( task_id='sum',
            ssh_hook=hook,
            env={
                'LSB_JOB_REPORT_MAIL': 'N',
            },
            retries=2,
            retry_delay=timedelta(seconds=1),
            lsf_script="""
#BSUB -o {{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( dag_run.conf['imaging_format'], '.job' ) }}
{% if params.convert_gainref %}#BSUB -w done({{ ti.xcom_pull( task_ids='convert_gainref' )['jobid'] }}){% endif %}
#BSUB -W 5
#BSUB -We 1
#BSUB -n 1

###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

mkdir -p {{ dag_run.conf['directory'] }}/summed/imod/4.9.4/
module load imod-4.9.4-gcc-4.8.5-ttohnna
cd -- "$( dirname {{ ti.xcom_pull( task_ids='stack_file' )[-1] }} )"
avgstack > {{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( dag_run.conf['imaging_format'], '.log' ) }} <<-'__AVGSTACK_EOF__'
{{ ti.xcom_pull( task_ids='stack_file' )[-1] }}
{%- if params.apply_gainref %}
/tmp/{{ dag_run.conf['base'] }}_avg.mrcs
/
__AVGSTACK_EOF__

# apply gainref
newstack  \
    {{ ti.xcom_pull( task_ids='gainref_file')[-1] | replace( '.dm4', '.mrc' ) }} \
    /tmp/{{ dag_run.conf['base'] }}_gainref.mrc
clip mult -n 16  \
    /tmp/{{ dag_run.conf['base'] }}_avg.mrcs \
    /tmp/{{ dag_run.conf['base'] }}_gainref.mrc \
    /tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc

{%- else %}
/tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc
/
__AVGSTACK_EOF__
{% endif %}

module load eman2-master-gcc-4.8.5-pri5spm
export PYTHON_EGG_CACHE='/tmp'
e2proc2d.py \
    /tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc  \
    {{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( dag_run.conf['imaging_format'], '.jpg' ) }} \
    --process filter.lowpass.gauss:cutoff_freq=0.05
cp -f /tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc {{ dag_run.conf['directory'] }}/summed/imod/4.9.4/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc
{%- if params.apply_gainref %}
rm -f /tmp/{{ dag_run.conf['base'] }}_avg.mrcs /tmp/{{ dag_run.conf['base'] }}_gainref.mrc
{% endif %}
rm -f /tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc
    """,
            params={
                'apply_gainref': args['apply_gainref'],
                'convert_gainref': args['convert_gainref'],
            }
        )

        influx_sum = LSFJob2InfluxOperator( task_id='influx_sum',
            job_name='sum',
            xcom_task_id='sum',
            host=args['influx_host'],
            experiment="{{ dag_run.conf['experiment'] }}",
        )

    summed_file = FileInfoSensor( task_id='summed_file',
        filepath="{% if params.daq_software == 'SerialEM' %}{{ dag_run.conf['directory'] }}/summed/imod/4.9.4/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc{% else %}{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}.mrc{% endif %}",
        params={
            'daq_software': args['daq_software'],
        },
        recursive=True,
        poke_interval=1,
    )

    logbook_summed_file = LogbookRegisterFileOperator( task_id='logbook_summed_file',
        file_info='summed_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    summed_preview = FileInfoSensor( task_id='summed_preview',
        filepath="{% if params.daq_software == 'SerialEM' %}{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( dag_run.conf['imaging_format'], '.jpg' ) }}{% else %}{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}.jpg{% endif %}",
        params={
            'daq_software': args['daq_software'],
        },
        recursive=True,
        poke_interval=1,
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
#BSUB -W 6
#BSUB -We 3
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
{% if 'superres' in dag_run.conf and dag_run.conf['superres'] %}{% if params.apix %}{{ params.apix | float / 2 }}{% else %}{{ dag_run.conf['apix'] | float / 2 }}{% endif %}{% else %}{% if params.apix %}{{ params.apix }}{% else %}{{ dag_run.conf['apix'] }}{% endif %}{% endif %}
{{ dag_run.conf['keV'] }}
{{ dag_run.conf['cs'] or 2.7 }}
0.1
512
30
4
1000
50000
200
no
no
yes
100
{% if 'phase_plate' in dag_run.conf and dag_run.conf['phase_plate'] %}yes
0
1.571
0.1
{%- else %}no{% endif %}
no
__CTFFIND_EOF__
""",
        params={
            'daq_software': args['daq_software'],
            'apix': args['apix'] if 'apix' in args else None,
        }
    )

    convert_summed_ctf_preview = LSFOperator( task_id='convert_summed_ctf_preview',
        ssh_hook=hook,
        poke_interval=1,
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/summed/ctffind4/4.1.8/{{ dag_run.conf['base'] }}_ctf_preview.job
#BSUB -w "done({{ ti.xcom_pull( task_ids='ctffind_summed' )['jobid'] }})"
#BSUB -W 5
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
e2proc2d.py --writejunk \
    {{ dag_run.conf['base'] }}_ctf.mrc \
    {{ dag_run.conf['base'] }}_ctf.jpg
""",
    )

    influx_summed_preview = LSFJob2InfluxOperator( task_id='influx_summed_preview',
        job_name='summed_preview',
        xcom_task_id='convert_summed_ctf_preview',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    ctf_summed = LSFJobSensor( task_id='ctf_summed',
        ssh_hook=hook,
        jobid="{{ ti.xcom_pull( task_ids='ctffind_summed' )['jobid'] }}",
        retries=2,
        retry_delay=timedelta(seconds=1),
        poke_interval=1,
    )

    influx_summed_ctf = LSFJob2InfluxOperator( task_id='influx_summed_ctf',
        job_name='ctf_summed',
        xcom_task_id='ctf_summed',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    summed_ctf_preview = FileInfoSensor( task_id='summed_ctf_preview',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_ctf.jpg",
        recursive=True,
        poke_interval=1,
    )

    summed_ctf_file = FileInfoSensor( task_id='summed_ctf_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_ctf.mrc",
        recursive=True,
        poke_interval=1,
    )

    logbook_summed_ctf_file= LogbookRegisterFileOperator( task_id='logbook_summed_ctf_file',
        file_info='summed_ctf_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )


    summed_ctf_data = Ctffind4DataSensor( task_id='summed_ctf_data',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_ctf.txt",
        recursive=True,
        poke_interval=1,
    )

    influx_summed_ctf_data = GenericInfluxOperator( task_id='influx_summed_ctf_data',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='stack_file' )[-1] }}",
        tags={
            'app': 'ctffind',
            'version': '4.1.8',
            'state': 'unaligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        tags2="{{ ti.xcom_pull( task_ids='summed_ctf_data', key='context' ) }}",
        fields="{{ ti.xcom_pull( task_ids='summed_ctf_data' ) }}",
    )

    resubmit_ctffind_summed = BashOperator( task_id='resubmit_ctffind_summed',
        trigger_rule='one_failed',
        bash_command="""
        airflow clear -t ctffind_summed -c -d -s {{ ts }} -e {{ ts }} {{ dag | replace( '<DAG: ', '' ) | replace( '>', '' ) }} &
        ( sleep 10; airflow clear -t resubmit_ctffind_summed -c -d -s {{ ts }} -e {{ ts }} {{ dag | replace( '<DAG: ', '' ) | replace( '>', '' ) }} ) &
        """
    )

    #clear_resubmit_ctffind_summed = BashOperator( task_id='clear_resubmit_ctffind_summed',
    #    bash_command="""
    #    airflow clear -t resubmit_ctffind_summed -c -d -s {{ ts }} -e {{ ts }} {{ dag | replace( '<DAG: ', '' ) | replace( '>', '' ) }}
    #    """
    #)

    convert_summed_ctf_preview >> resubmit_ctffind_summed
    ctf_summed >> resubmit_ctffind_summed

    #ctffind_summed >> clear_resubmit_ctffind_summed

    ###
    #
    ###
    stack_file = FileInfoSensor( task_id='stack_file',
        # filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}%s" % ( '-*%s' % args['stack_file_format'] if args['stack_file_format'] == '.mrc' else '*%s' % args['stack_file_format'],),
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}{% if dag_run.conf['imaging_format'] == '.mrc' %}-*.mrc{% elif dag_run.conf['imaging_format'] == '.tif' %}*.tif{% endif %}",
        recursive=True,
        excludes=['gain-ref',],
        poke_interval=1,
    )
    
    logbook_stack_file = LogbookRegisterFileOperator( task_id='logbook_stack_file',
        file_info='stack_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )
    

    if args['convert_gainref']:

        gainref_file = FileInfoSensor( task_id='gainref_file',
            filepath="{{ dag_run.conf['directory'] }}/**/{% if params.daq_software == 'SerialEM' %}{% if 'superres' in dag_run.conf and dag_run.conf['superres'] %}Super{% else %}Count{% endif %}Ref*.dm4{% else %}{{ dag_run.conf['base'] }}-gain-ref.dm4{% endif %}",
            params={
                'daq_software': args['daq_software'],
            },
            recursive=True,
            poke_interval=1,
        )
        
        logbook_gainref_file = LogbookRegisterFileOperator( task_id='logbook_gainref_file',
            file_info='gainref_file',
            http_hook=logbook_hook,
            experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
            run="{{ dag_run.conf['base'] }}"
        )


##### {% if params.daq_software == 'EPU' %}
##### {% else %}
###
# convert using imod
###
# cd {{ dag_run.conf['directory'] }}
#if [ ! -f {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }} ]; then
#  module load imod-4.9.4-intel-17.0.2-fdpbjp4
#  dm2mrc  {{ ti.xcom_pull( task_ids='gainref_file' )[0] }} {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }}
#fi
##### {% endif %}

        ####
        # convert gain ref to mrc
        ####
        convert_gainref = LSFSubmitOperator( task_id='convert_gainref',
            ssh_hook=hook,
            env={
                'LSB_JOB_REPORT_MAIL': 'N',
            },
            lsf_script="""
#!/bin/bash

#BSUB -o {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.job' ) }}
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
# convert using eman2
###
if [ ! -f {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }} ]; then
  module load eman2-master-gcc-4.8.5-pri5spm
  export PYTHON_EGG_CACHE='/tmp'
  cd -- "$( dirname {{ ti.xcom_pull( task_ids='gainref_file' )[0] }} )"
  echo e2proc2d.py {% if params.rotate_gainref > 0 %}--rotate {{ params.rotate_gainref }}{% endif %}{{ ti.xcom_pull( task_ids='gainref_file' )[0] }} {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }}
  e2proc2d.py {% if params.rotate_gainref > 0 %}--rotate {{ params.rotate_gainref }}{% endif %}{{ ti.xcom_pull( task_ids='gainref_file' )[0] }} {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }}
fi
            """,
            params={
                'daq_software': args['daq_software'],
                'rotate_gainref': 0, #args['rotate_gainref'],
            }
                
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

    if args['apply_gainref']:

        new_gainref_file = FileInfoSensor( task_id='new_gainref_file',
            filepath="{% if not params.convert_gainref %}{{ dag_run.conf['directory'] }}/**/gain-ref.mrc{% else %}{{ dag_run.conf['directory'] }}/**/{% if params.daq_software == 'SerialEM' %}{% if 'superres' in dag_run.conf and dag_run.conf['superres'] %}Super{% else %}Count{% endif %}Ref*.mrc{% else %}{{ dag_run.conf['base'] }}-gain-ref.mrc{% endif %}{% endif %}",
            recursive=True,
            params={
                'daq_software': args['daq_software'],
                'convert_gainref': args['convert_gainref']
            },
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
        lsf_script="""
#BSUB -R "select[ngpus>0] rusage[ngpus_excl_p=1]"
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/{{ dag_run.conf['base'] }}_aligned.job
{% if params.convert_gainref %}#BSUB -w "done({{ ti.xcom_pull( task_ids='convert_gainref' )['jobid'] }})"{% endif %}
#BSUB -W 15
#BSUB -We 7
#BSUB -n 1
###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# align the frames
###
module load motioncor2-1.0.5-gcc-4.8.5-fxpzvf2
mkdir -p {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/
MotionCor2  \
    -In{% if dag_run.conf['imaging_format'] == '.mrc' %}Mrc{% elif dag_run.conf['imaging_format'] == '.tif' %}Tiff{% endif %} {{ ti.xcom_pull( task_ids='stack_file' )[-1] }} \
{% if params.apply_gainref %}{% if params.convert_gainref %}   -Gain {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }} {% else %}    -Gain {{ ti.xcom_pull( task_ids='new_gainref_file' )[-1] }} {% endif %}{% endif -%}\
    -OutMrc   {{ dag_run.conf['base'] }}_aligned.mrc \
    -LogFile  {{ dag_run.conf['base'] }}_aligned.log \
    -kV       {{ dag_run.conf['keV'] }} \
    -FmDose   {{ dag_run.conf['fmdose'] }} \
    -Bft      {% if 'preprocess/align/motioncor2/bft' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/bft'] }}{% else %}150{% endif %} \
    -PixSize  {% if 'superres' in dag_run.conf and dag_run.conf['superres'] %}{% if params.apix %}{{ params.apix | float / 2 }}{% else %}{{ dag_run.conf['apix'] | float / 2 }}{% endif %}{% else %}{% if params.apix %}{{ params.apix }}{% else %}{{ dag_run.conf['apix'] }}{% endif %}{% endif %} \
    -FtBin    {% if 'superres' in dag_run.conf and dag_run.conf['superres'] %}2{% else %}1{% endif %} \
    -Patch    {% if 'preprocess/align/motioncor2/patch' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/patch'] }}{% else %}5 5{% endif %} \
    -Throw    {% if 'preprocess/align/motioncor2/throw' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/throw'] }}{% else %}0{% endif %} \
    -Trunc    {% if 'preprocess/align/motioncor2/trunc' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/trunc'] }}{% else %}0{% endif %} \
    -Iter     {% if 'preprocess/align/motioncor2/iter' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/iter'] }}{% else %}10{% endif %} \
    -OutStack {% if 'preprocess/align/motioncor2/outstack' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/outstack'] }}{% else %}0{% endif %}  \
    -Gpu      {{ params.gpu }}
""",
        params={
            'gpu': 0,
            'apply_gainref': args['apply_gainref'],
            'convert_gainref': args['convert_gainref'],
            'apix': args['apix'] if 'apix' in args else None,
        },
    )
    
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

    drift_data = MotionCor2DataSensor( task_id='drift_data',
        filepath="{{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/{{ dag_run.conf['base'] }}_aligned.log0-Patch-Full.log",
        poke_interval=5,
    )

    influx_drift_data = GenericInfluxOperator( task_id='influx_drift_data',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='stack_file' )[-1] }}",
        tags={
            'app': 'motioncor2',
            'version': '1.0.5',
            'state': 'aligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        fields="{{ ti.xcom_pull( task_ids='drift_data' ) }}",
    )

    # if args['output_aligned_movie_stack']:
    #     aligned_stack_file = FileInfoSensor( task_id='aligned_stack_file',
    #        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned_Stk.mrc",
    #        recursive=True,
    #        poke_interval=5,
    #     )


    convert_aligned_preview = LSFOperator( task_id='convert_aligned_preview',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/{{ dag_run.conf['base'] }}_aligned_preview.job
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
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/
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

    aligned_file = FileInfoSensor( task_id='aligned_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned.mrc",
        recursive=True,
        poke_interval=1,
    )
    
    logbook_aligned_file = LogbookRegisterFileOperator( task_id='logbook_aligned_file',
        file_info='aligned_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    aligned_preview = FileInfoSensor( task_id='aligned_preview',
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
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/ctffind4/4.1.8/{{ dag_run.conf['base'] }}_aligned_ctf.job
{% if True %}#BSUB -w "done({{ ti.xcom_pull( task_ids='motioncorr_stack' )['jobid'] }})"{% endif %}
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
mkdir -p {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/ctffind4/4.1.8/
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/ctffind4/4.1.8/
ctffind > {{ dag_run.conf['base'] }}_aligned_ctf.log <<-'__CTFFIND_EOF__'
{{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/{{ dag_run.conf['base'] }}_aligned.mrc
{{ dag_run.conf['base'] }}_aligned_ctf.mrc
{% if params.apix %}{{ params.apix }}{% else %}{{ dag_run.conf['apix'] }}{% endif %}
{{ dag_run.conf['keV'] }}
{{ dag_run.conf['cs'] or 2.7 }}
0.1
512
30
4
1000
50000
200
no
no
yes
100
{% if 'phase_plate' in dag_run.conf and dag_run.conf['phase_plate'] %}yes
0
1.571
0.1
{%- else %}no{% endif %}
no
__CTFFIND_EOF__
""",
        params={
            'apix': args['apix'] if 'apix' in args else None,
        }
    )

    convert_aligned_ctf_preview = LSFOperator( task_id='convert_aligned_ctf_preview',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
        },
        poke_interval=1,
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/ctffind4/4.1.8/{{ dag_run.conf['base'] }}_aligned_ctf.job
#BSUB -w "done({{ ti.xcom_pull( task_ids='ctffind_aligned' )['jobid'] }})"
#BSUB -W 5 
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
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.0.5/ctffind4/4.1.8/
e2proc2d.py \
    {{ dag_run.conf['base'] }}_aligned_ctf.mrc \
    {{ dag_run.conf['base'] }}_aligned_ctf.jpg
""",
    )

    influx_ctf_preview = LSFJob2InfluxOperator( task_id='influx_ctf_preview',
        job_name='ctf_preview',
        xcom_task_id='convert_aligned_ctf_preview',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )


    ctf_aligned = LSFJobSensor( task_id='ctf_aligned',
        ssh_hook=hook,
        jobid="{{ ti.xcom_pull( task_ids='ctffind_aligned' )['jobid'] }}",
        retries=2,
        retry_delay=timedelta(seconds=1),
        poke_interval=1,
    )

    influx_ctf_aligned = LSFJob2InfluxOperator( task_id='influx_ctf_aligned',
        job_name='ctf_aligned',
        xcom_task_id='ctf_aligned',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    aligned_ctf_file = FileInfoSensor( task_id='aligned_ctf_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned_ctf.mrc",
        recursive=True,
        poke_interval=1,
    )
    
    logbook_aligned_ctf_file = LogbookRegisterFileOperator( task_id='logbook_aligned_ctf_file',
        file_info='aligned_ctf_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    aligned_ctf_preview = FileInfoSensor( task_id='aligned_ctf_preview',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned_ctf.jpg",
        recursive=1,
        poke_interval=1,
    )

    aligned_ctf_data = Ctffind4DataSensor( task_id='aligned_ctf_data',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_aligned_ctf.txt",
        recursive=True,
    )

    influx_aligned_ctf_data = GenericInfluxOperator( task_id='influx_aligned_ctf_data',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='stack_file' )[-1] }}",
        tags={
            'app': 'ctffind',
            'version': '4.1.8',
            'state': 'aligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        tags2="{{ ti.xcom_pull( task_ids='aligned_ctf_data', key='context' ) }}",
        fields="{{ ti.xcom_pull( task_ids='aligned_ctf_data' ) }}",
    )

    previews = BashOperator( task_id='previews',
        bash_command="""
            # summed preview
            mkdir -p {{ dag_run.conf['directory'] }}/summed/previews
            cd {{ dag_run.conf['directory'] }}/summed/previews/
            convert \
                -resize 512x495 \
                {{ ti.xcom_pull( task_ids='summed_preview' )[0] }} \
                {{ ti.xcom_pull( task_ids='summed_ctf_preview' )[0] }} \
                +append -pointsize 36 -fill yellow -draw 'text 814,478 \"{{ '%0.1f' | format(ti.xcom_pull( task_ids='summed_ctf_data' )['resolution']) }}Å ({{ '%d' | format(ti.xcom_pull( task_ids='summed_ctf_data' )['resolution_performance'] * 100) }}%)\"' \
                {{ dag_run.conf['base'] }}_sidebyside.jpg

            # aligned preview
            mkdir -p {{ dag_run.conf['directory'] }}/aligned/previews/
            cd {{ dag_run.conf['directory'] }}/aligned/previews/
            convert \
                -resize 512x495 \
                {{ ti.xcom_pull( task_ids='aligned_preview' )[0] }} \
                {{ ti.xcom_pull( task_ids='aligned_ctf_preview' )[0] }} \
                +append \
                -pointsize 36 -fill orange -draw 'text 402,46 \"{{ '%0.3f' | format(ti.xcom_pull( task_ids='drift_data' )['drift']) }}\"' \
                +append  \
                -pointsize 36 -fill orange -draw 'text 814,46 \"{{ '%0.1f' | format(ti.xcom_pull( task_ids='aligned_ctf_data' )['resolution']) }}Å ({{ '%d' | format(ti.xcom_pull( task_ids='aligned_ctf_data' )['resolution_performance'] * 100) }}%)\"' \
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

    previews_file = FileInfoSensor( task_id='previews_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}_full_sidebyside.jpg"
    )

    logbook_previews_file = LogbookRegisterFileOperator( task_id='logbook_previews_file',
        file_info='previews_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    slack_full_preview = SlackAPIUploadFileOperator( task_id='slack_full_preview',
        channel="{{ dag_run.conf['experiment'][:21] | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
        filepath="{{ dag_run.conf['directory'] }}/previews/{{ dag_run.conf['base'] }}_full_sidebyside.jpg",
        retries=2,
    )

    logbook_run_params = LogbookRegisterRunParamsOperator(task_id='logbook_run_params',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    resubmit_stack = BashOperator( task_id='resubmit_stack',
        trigger_rule='all_failed',
        bash_command="""
            airflow clear -t {% if params.convert_gainref %}convert_gainref{% else %}motioncorr_stack{% endif %} -c -d -s {{ ts }} -e {{ ts }} {{ dag | replace( '<DAG: ', '' ) | replace( '>', '' ) }} &
            ( sleep 10; airflow clear -t resubmit_stack -c -d -s {{ ts }} -e {{ ts }} {{ dag | replace( '<DAG: ', '' ) | replace( '>', '' ) }} ) &
        """,
        params={
            'convert_gainref': args['convert_gainref'],
        },
    )

    clear_resubmit_stack = BashOperator( task_id='clear_resubmit_stack',
        bash_command="""
        airflow clear -t resubmit_stack -c -d -s {{ ts }} -e {{ ts }} {{ dag | replace( '<DAG: ', '' ) | replace( '>', '' ) }} &
        ( sleep 10 ) &
        """
    )

    resubmit_stack << align
    resubmit_stack << convert_aligned_preview
    resubmit_stack << convert_aligned_ctf_preview
    resubmit_stack << ctf_aligned
    #if args['convert_gainref']:
    #    resubmit_stack << new_gainref

    motioncorr_stack >> clear_resubmit_stack

    ###
    # define pipeline
    ###

    if args['daq_software'] == 'EPU':
        parameter_file >> parse_parameters >> logbook_parameters
        summed_preview  >> logbook_parameters
        parse_parameters >> influx_parameters
        parse_parameters >> ctffind_summed
        # stack_file >> summed_preview
        
    elif args['daq_software'] == 'SerialEM':
        stack_file >> sum >> summed_preview
        sum >> summed_file
        sum >> influx_sum
        
    stack_file >> logbook_stack_file
        
    summed_file >> ctffind_summed
    summed_file >> logbook_summed_file
    
    ctffind_summed >> ctf_summed
    
    ctffind_summed >> convert_summed_ctf_preview >> influx_summed_preview
    ctf_summed >> influx_summed_ctf

    ensure_slack_channel >> invite_slack_users
    
    previews >> previews_file >> logbook_previews_file
    summed_preview >> previews
    summed_ctf_preview >> previews

    ctf_summed >> logbook_run_params
    convert_summed_ctf_preview >> summed_ctf_preview
    ctf_summed >> summed_ctf_file >> logbook_summed_ctf_file
    ctf_summed >> summed_ctf_data

    summed_ctf_data >> previews
    summed_ctf_data >> influx_summed_ctf_data

    create_run = LogbookCreateRunOperator( task_id='create_run',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    create_run >> stack_file 
    create_run >> gainref_file >> logbook_gainref_file
    
    stack_file >> motioncorr_stack >> convert_aligned_preview

    if args['convert_gainref']:
        gainref_file >> convert_gainref
        new_gainref >> influx_new_gainref
        convert_gainref >> new_gainref
        new_gainref >> new_gainref_file

    if args['apply_gainref']:
        if not args['convert_gainref']:
            new_gainref_file >> motioncorr_stack
            if args['daq_software'] == 'SerialEM':
                new_gainref_file >> sum
        else:
            convert_gainref >> motioncorr_stack
            if args['daq_software'] == 'SerialEM':
                convert_gainref >> sum       

    motioncorr_stack >> align
    #align >> aligned_stack_file
    align >> influx_aligned
    align >> drift_data >> influx_drift_data 
    drift_data >> previews

    ctf_aligned >> aligned_ctf_file >> logbook_aligned_ctf_file
    convert_aligned_ctf_preview >> aligned_ctf_preview
    convert_aligned_ctf_preview >> influx_ctf_preview

    ctf_aligned >> aligned_ctf_data
    aligned_ctf_data >> previews
    aligned_ctf_data >> influx_aligned_ctf_data

    align >> logbook_run_params

    align >> aligned_file >> logbook_aligned_file
    motioncorr_stack >> ctffind_aligned >> ctf_aligned >> logbook_run_params
    ctffind_aligned >> convert_aligned_ctf_preview
    convert_aligned_preview >> aligned_preview
    convert_aligned_preview >> influx_aligned_preview

    aligned_preview >> previews
    aligned_ctf_preview >> previews

    ensure_slack_channel >> slack_full_preview
    previews >> slack_full_preview

    ctf_aligned >> influx_ctf_aligned

