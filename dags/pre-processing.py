
from airflow import utils
from airflow import DAG

from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import BaseSensorOperator

from airflow.operators import FileSensor

from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators import SlackAPIEnsureChannelOperator, SlackAPIInviteToChannelOperator, SlackAPIUploadFileOperator

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
    'slack_channel': 'test'
}







def conv(v):
    """ try to parse v to a meaningful type """
    a = v
    try:
        a = literal_eval(v)
    except:
        pass
        if v in ( 'true', 'True' ):
            a = True
        elif v in ( 'false', 'False' ):
            a = False
        #     a = bool(v)
    return a

# def conv(v):
#     return v

# adapted from https://stackoverflow.com/questions/13412496/python-elementtree-module-how-to-ignore-the-namespace-of-xml-files-to-locate-ma
def etree_to_dict(t):
    """ convert the etree into a python dict """
    tag = t.tag
    if '}' in tag:
        tag = t.tag.split('}', 1)[1]
    # LOG.info("TAG: %s" % tag)
    d = {tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {tag: {k: conv(v[0]) if len(v) == 1 else v
                     for k, v in dd.items()}}
        if tag == 'KeyValueOfstringanyType':
            this_k = None
            v = None
            for i,j in dd.items():
                # LOG.info("  HERE %s -> %s" % (i,j))
                for a in j:
                    # LOG.info("    THIS %s" % a)
                    if i == 'Key':
                        this_k = a                   
                    elif i == 'Value' and isinstance(a,dict) and '#text' in a:
                        # LOG.info("      %s = %s" % (this_k,a['#text']))
                        v = a['#text']
                    else:
                        v = a
            # LOG.info("+++ %s = %s" % (this_k,v))
            d = { this_k: conv(v) }
        
        # loosing units?
        elif 'numericValue' in dd:
            # LOG.info("FOUND numericValue: %s %s" % (tag,dd))
            d = { tag: dd['numericValue'][0] }

        # remove the units
        elif tag == 'ReferenceTransformation':
            # LOG.info("HERE %s" % dd)
            del d['ReferenceTransformation']['unit']

    if t.attrib:
        # d[tag].update(('@' + k, v)
        #                 for k, v in t.attrib.items())
        for k, v in t.attrib.items():
            if '}' in k:
                k = k.split('}', 1)[1]
            d[tag]['@' + k] = conv(v)
            # deal with @nil's
            if k == 'nil':
                # LOG.info("FOUND %s = %s" % (k, v))
                d[tag] = conv(v)

    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[tag]['#text'] = text
        else:
            d[tag] = text
    return d


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



def get_filepath( extention='mrc', **kwargs ):
    d = kwargs['dag_run'].conf['directory']
    f = kwargs['dag_run'].conf['base']
    filepath = Path( '%s/%s.%s' % (d,f,extention) )
    if filepath.is_file():
        return filepath
    else:
        raise AirflowException('File %s does not exist' % (filepath) )
    

def uploadExperimentalParameters2Logbook(ds, **kwargs):
    """Push the parameter key-value pairs to the elogbook"""
    data = kwargs['ti'].xcom_pull( task_ids='parse_parameters', key='return_value' )
    LOG.warn("data: %s" % (data,))
    raise AirflowSkipException('not yet implemented')

def uploadExperimentalParameters2Influx(ds, measurement='cryoem', **kwargs):
    """Push the parameter key-value pairs to the elogbook"""
    d = kwargs['ti'].xcom_pull( task_ids='parse_parameters', key='return_value' )
    LOG.warn("data: %s" % (data,))
    dt = parser.parse( d['microscopeData']['acquisition']['acquisitionDateTime'] )
    # LOG.info("DT %s" % dt)
    dd = flatten(d, sep='_')
    context = []
    data = []
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
            context.append("%s=%s"%(k,vv))
        else:
            # LOG.info("  data %s" % v)
            data.append( '%s=%s'%(k,float(v)) )

    # LOG.info("CONTEXT: %s" % (sorted(context),))
    # LOG.info("DATA: %s" % (sorted(data),))

    return '%s,%s %s %s' % (measurement,','.join(sorted(context)), ','.join(sorted(data)), dt.strftime('%s'))



def motioncorr(context, **kwargs):
    """Run motioncorr on the data files and xcom push all of the data"""
    filepath = get_filepath( extention='mrc', **kwargs )
    LOG.warn("motioncorr on %s" % (filepath,))
    raise AirflowSkipException('not yet implemented')
    
    
def ctffind(context, **kwargs):
    """Run ctffind on the data files and xcom push all of the data"""
    filepath = get_filepath( extention='mrc', **kwargs )
    LOG.warn("ctffind on %s" % (filepath,))
    raise AirflowSkipException('not yet implemented')






def parseEPUMetaData(ds, **kwargs):
    """ read the experimental parameters from an FEI xml file """
    # filepath = get_filepath( extention='xml', **kwargs )
    filepath = kwargs['ti'].xcom_pull( task_ids='wait_for_parameters', key='file')
    LOG.info('parsing fei epu xml file %s' % (filepath,))
    return etree_to_dict( ET.parse( filepath ).getroot() )



###
# define the workflow
###
with DAG( 'cryoem_pre-processing',
        description="Conduct some initial processing to determine efficacy of CryoEM data and upload it to the elogbook",
        schedule_interval=None,
        default_args=args,
        max_active_runs=1
    ) as dag:


    ###
    # parse the epu xml metadata file
    ###
    t_wait_params = FileSensor( task_id='wait_for_parameters',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.xml",
        timeout=10,
        poke=3,
    )
    t_parameters = PythonOperator(task_id='parse_parameters',
        python_callable=parseEPUMetaData,
        provide_context=True,
    )
    # upload to the logbook
    t_param_logbook = PythonOperator(task_id='upload_parameters_to_logbook',
        python_callable=uploadExperimentalParameters2Logbook,
        op_kwargs={}
    )
    # upload to influxdb
    t_param_influx = PythonOperator(task_id='upload_parameters_to_influx',
        python_callable=uploadExperimentalParameters2Influx,
        op_kwargs={}
    )

    t_ensure_slack_channel = SlackAPIEnsureChannelOperator( task_id='ensure_slack_channel',
        channel=args['slack_channel'],
        token=Variable.get('slack_token'),
    )
    t_invite_to_slack_channel = SlackAPIInviteToChannelOperator( task_id='invite_slack_users',
        channel=args['slack_channel'],
        token=Variable.get('slack_token'),
        users=('yee',),
    )


    ###
    # get the summed jpg
    ###
    t_wait_preview = FileSensor( task_id='wait_for_preview',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.jpg",
        timeout=10,
        poke=3,
    )
    t_slack_preview = SlackAPIUploadFileOperator( task_id='slack_preview',
        channel=args['slack_channel'],
        token=Variable.get('slack_token'),
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.jpg",
    )    

    ###
    # get the summed mrc
    ###
    t_wait_summed = FileSensor( task_id='wait_for_summed',
        filepath="{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.mrc",
        timeout=10,
        poke=3,
    )
    # TODO need parameters for input into ctffind
    t_ctf_summed = BashOperator( task_id='ctf_summed',
        bash_command="""
echo ctffind > {{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}_ctffind4.log << EOF
{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.mrc
{{ dag_run.conf['directory'] }}/{{ dag_run.conf['base'] }}.ctf
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
EOF
""",
        params={
            'voltage': 300,
            'pixel_size': 1.246,
            'Cs': 2.7,
            
        }
    )
    t_slack_summed_ctf = SlackAPIPostOperator( task_id='slack_summed_ctf',
        channel=args['slack_channel'],
        token=Variable.get('slack_token'),
        text='ctf! ctf!'
    )

    t_ctf_summed_logbook = DummyOperator( task_id='upload_summed_ctf_logbook' )

    ###
    #
    ###
    def tif2mrc(**kwargs):
        """Convert the tif file to mrc"""
        pass
    t_tif2mrc = PythonOperator(task_id='convert_tif_to_mrc',
        python_callable=tif2mrc,
        op_kwargs={}
    )

    ###
    # align the frame
    ###
    t_motioncorr = PythonOperator(task_id='motioncorr',
        python_callable=motioncorr,
        op_kwargs={}
    )

    t_motioncorr_2_logbbok = DummyOperator(task_id='upload_motioncorr_to_logbook')


    t_ctffind = PythonOperator(task_id='ctffind',
        python_callable=ctffind,
        op_kwargs={}
    )


    t_ctffind_2_logbook = DummyOperator(task_id='upload_ctffind_to_logbook')



    ###
    # define pipeline
    ###

    t_wait_params >> t_parameters >> t_param_logbook 
    t_wait_preview  >> t_param_logbook
    t_wait_preview >> t_slack_preview
    t_parameters >> t_param_influx 

    t_ensure_slack_channel >> t_invite_to_slack_channel
    t_ensure_slack_channel >> t_slack_preview
    t_ensure_slack_channel >> t_slack_summed_ctf

    t_wait_summed >> t_ctf_summed >> t_ctf_summed_logbook 
    t_ctf_summed >> t_slack_summed_ctf

    t_tif2mrc >> t_motioncorr >> t_motioncorr_2_logbbok 

    t_tif2mrc >> t_ctffind >> t_ctffind_2_logbook 

