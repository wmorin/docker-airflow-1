
from airflow import utils
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
from airflow.operators import FeiEpuOperator

from airflow.exceptions import AirflowException, AirflowSkipException

import os
import sys
import fileinput
from pathlib import Path
import requests

import influxdb
from pprint import pprint, pformat

import json
from collections import defaultdict, MutableMapping
import xml.etree.ElementTree as ET
from ast import literal_eval
from dateutil import parser

import logging
LOG = logging.getLogger(__name__)

args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': utils.dates.days_ago(0),
    'ssh_connection_id': 'ssh_docker_host',
}



# from https://stackoverflow.com/questions/6027558/flatten-nested-python-dictionaries-compressing-keys
def flatten(d, parent_key='', sep='.'):
    """ flatten the dict d so that we can export as key/value pairs """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)



def uploadExperimentalParameters2Logbook(ds, **kwargs):
    """Push the parameter key-value pairs to the elogbook"""
    data = kwargs['ti'].xcom_pull( task_ids='parse_parameters' )
    LOG.warn("data: %s" % (data,))
    raise AirflowSkipException('not yet implemented')


class Send2InfluxOperator(PythonOperator):
    template_fields = ('experiment',)
    def __init__(self,xcom_task_id,xcom_key,host='localhost',port=8086,user='root',password='root',db='cryoem',measurement='microscope_image',experiment=None,*args,**kwargs):
        BaseOperator.__init__(self,*args,**kwargs)
        self.xcom_task_id = xcom_task_id
        self.xcom_key = xcom_key
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.measurement = measurement
        self.experiment = experiment
    def execute(self, context,**kwargs):
        """Push the parameter key-value pairs to the elogbook"""
        d = context['ti'].xcom_pull( task_ids=self.xcom_task_id, key=self.xcom_key )['MicroscopeImage']
        dt = parser.parse( d['microscopeData']['acquisition']['acquisitionDateTime'] )
        dd = flatten(d, sep='_')
        context = {}
        data = {} 
        for k,v in dd.items():
            # ignore these entries
            if k in ( 'microscopeData_acquisition_acquisitionDateTime', 'CustomData_FindFoilHoleCenterResults_@type' ):
                continue
            # force context
            elif k in ( 'microscopeData_instrument_InstrumentID', ):
                v = '%s' % v
            # LOG.info("k=%s, v=%s" % (k,v))
            if isinstance( v, (str,bool) ) or v == None:
                # LOG.info("  context")
                vv = "'%s'" % v if isinstance(v,str) and ' ' in v else v
                context[k] = vv
            else:
                # LOG.info("  data %s" % v)
                data[k] = float(v)

        client = influxdb.InfluxDBClient( self.host, self.port, self.user, self.password, self.db )
        #LOG.info("DB: %s:%s/%s (%s %s)" % (self.host, self.port, self.db, self.user, self.password) )
        #LOG.info( '%s @ %s: \n%s\n%s' % (self.measurement,dt.strftime('%s'),pformat(context),pformat(data) ) )
        client.create_database(self.measurement)
        if self.experiment:
           context['experiment'] = self.experiment
        client.write_points([{
            "measurement": self.measurement,
            "tags": context,
            "fields": data,
            "time": dt,
        }])
        return




###
# define the workflow
###
with DAG( 'cryoem_pre-processing',
        description="Conduct some initial processing to determine efficacy of CryoEM data and upload it to the elogbook",
        schedule_interval=None,
        default_args=args,
        max_active_runs=3
    ) as dag:

    # hook to container host for lsf commands
    hook = SSHHook(conn_id=args['ssh_connection_id'])

    ###
    # parse the epu xml metadata file
    ###
    t_wait_params = FileSensor( task_id='wait_for_parameters',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.xml",
    )
    t_parameters = FeiEpuOperator(task_id='parse_parameters',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.xml",
    )
    # upload to the logbook
    t_param_logbook = PythonOperator(task_id='upload_parameters_to_logbook',
        python_callable=uploadExperimentalParameters2Logbook,
        op_kwargs={}
    )
    t_param_influx = Send2InfluxOperator( task_id='upload_parameters_to_influx',
        xcom_task_id='parse_parameters',
        xcom_key='return_value',
        host='influxdb01.slac.stanford.edu',
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    t_ensure_slack_channel = SlackAPIEnsureChannelOperator( task_id='ensure_slack_channel',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
    )
    t_invite_to_slack_channel = SlackAPIInviteToChannelOperator( task_id='invite_slack_users',
        channel="{{ dag_run.conf['experiment'][:21] }}",
        token=Variable.get('slack_token'),
        users=('yee',),
    )


    ###
    # get the summed jpg
    ###
    t_wait_preview = FileSensor( task_id='wait_for_preview',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.jpg",
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
        lsf_script="""
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
{{ params.voltage }}
{{ params.Cs }}
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
module load imod-4.9.4-intel-17.0.2-fdpbjp4
mrc2tif -j {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.mrc {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctf.jpg

        """,
        params={
            'voltage': 300,
            'pixel_size': 1.246,
            'Cs': 2.7,
            
        }
    )
    
    t_wait_ctf_summed = LSFJobSensor( task_id='wait_ctf_summed',
        ssh_hook=hook,
        bjobs="/afs/slac/package/lsf/curr/bin/bjobs",
        jobid="{{ ti.xcom_pull( task_ids='ctf_summed' ) }}"
    )
    
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



    t_ctf_summed_logbook = DummyOperator( task_id='upload_summed_ctf_logbook' )


    t_summed_sidebyside = DummyOperator( task_id='summed_sidebyside',
        # take the jpg and the ctf jpg and put it togehter to upload
    )

    t_slack_summed_sidebyside = DummyOperator( task_id='slack_summed_sideby_side',
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
        lsf_script="""
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
dm2mrc {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.dm4 {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.mrc

""",
    )
    t_wait_job_new_gainref = LSFJobSensor( task_id='wait_new_gainref',
        ssh_hook=hook,
        bjobs="/afs/slac/package/lsf/curr/bin/bjobs",
        jobid="{{ ti.xcom_pull( task_ids='convert_gain_ref' ) }}"
    )


    t_wait_new_gainref = FileSensor( task_id='wait_for_new_gainref',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.mrc }}"
    )


    ###
    # align the frame
    ###
    t_motioncorr_stack = LSFSubmitOperator( task_id='motioncorr_stack',
        ssh_hook=hook,
        queue_name="ocio-gpu",
        bsub='/afs/slac/package/lsf/curr/bin/bsub',
        lsf_script="""
###
# boostrap - not sure why i need this for it to work when running from cryoem-airflow
###
module() { eval `/usr/bin/modulecmd bash $*`; }
export -f module
export MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-rhel7-x86_64

###
# align the frames
###  
MotionCor2  -InMrc {{ ti.xcom_pull( task_ids='wait_for_stack' ).pop(0) }} -OutMrc {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.mrc -LogFile {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_aligned.log }} -Gain {{ params.gain_ref_file }} -Bft {{ params.bft }} -PixSize {{ params.pixel_size }} -OutStack 1 -Patch {{ params.patch }} -Gpu {{ params.gpu }}
        """,
        params={
            'gain_ref_file': "{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}-gain-ref.dm4 }}",
            'bft': 150,
            'pixel_size': 1.286,
            'patch': '5 5',
            'gpu': 0,
            
        }
    )

    t_wait_motioncorr_stack = LSFJobSensor( task_id='wait_motioncor_stack',
        ssh_hook=hook,
        bjobs="/afs/slac/package/lsf/curr/bin/bjobs",
        jobid="{{ ti.xcom_pull( task_ids='motioncorr_stack' ) }}"
    )

    t_motioncorr_2_logbbok = DummyOperator(task_id='upload_motioncorr_to_logbook')


    t_wait_for_aligned = FileSensor( task_id='wait_for_aligned',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_stack.mrc }}",
    )



    t_ctffind_stack = LSFSubmitOperator( task_id='ctffind_stack',
        ssh_hook=hook,
        queue_name="ocio-gpu",
        bsub='/afs/slac/package/lsf/curr/bin/bsub',
        lsf_script="""
"""
    )

    t_wait_ctffind_stack = LSFJobSensor( task_id='wait_ctffind_stack',
        ssh_hook=hook,
        bsub='/afs/slac/package/lsf/curr/bin/bjobs',
        jobid="{{ ti.xcom_pull( task_ids='ctffind_stack' ) }}"
    )
    
    t_ctffind_stack_logbook = DummyOperator(task_id='upload_ctffind_to_logbook')



    ###
    # define pipeline
    ###

    t_wait_params >> t_parameters >> t_param_logbook 
    t_wait_preview  >> t_param_logbook
    t_wait_preview >> t_slack_preview
    t_parameters >> t_param_influx 

    t_parameters >> t_ctf_summed >> t_wait_ctf_summed
    

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
    t_wait_new_gainref >> t_motioncorr_stack
    t_motioncorr_stack >> t_wait_motioncorr_stack 

    t_wait_motioncorr_stack >> t_ctffind_stack
    t_wait_motioncorr_stack >> t_motioncorr_2_logbbok 

    t_wait_for_aligned >> t_ctffind_stack >> t_wait_ctffind_stack >> t_ctffind_stack_logbook 

