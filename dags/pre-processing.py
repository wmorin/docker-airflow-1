
from airflow import utils
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import sys
import fileinput
from pprint import pprint, pformat
import json
from collections import defaultdict, MutableMapping
import xml.etree.ElementTree as ET
from ast import literal_eval
from dateutil import parser


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


def parseFEIMetaData(**kwargs):
    """ read the experimental parameters from an FEI xml file """
    d = etree_to_dict( ET.parse(sys.argv[1]).getroot() )
    return d


###
# define the workflow
###
with DAG( 'cryoem_pre-processing',
        description="Conduct some initial processing to determine efficacy of CryoEM data and upload it to the elogbook",
        schedule_interval=None,
        start_date=utils.dates.days_ago(0),
        default_args=args,
        max_active_runs=1
    ) as dag:


    # do i want a file sensor? or do i want to read it in from an external DAG trigger?
    # pros/cons?
    # using a file sensor to monitor for new files would allow this is to run indenpendently; however, we would need some state to be recorded so that we may ignore stuff that has already been processed or is currently processing (latter hard)


    t_parameters = PythonOperator(task_id='determine_tem_parameters',
        python_callable=parseFEIMetaData,
        op_kargs={}
    )


    def uploadExperimentalParameters(**kwargs):
        """Push the parameter key-value pairs to the elogbook"""
        pass
    t_logbook = PythonOperator(task_id='upload_parameters_to_logbook',
        python_callable=uploadExperimentalParameters,
        op_kwargs={}
    )


    def tif2mrc(**kwargs):
        """Convert the tif file to mrc"""
        pass
    t_tif2mrc = PythonOperator(task_id='convert_tif_to_mrc',
        python_callable=tif2mrc,
        op_kwargs={}
    )


    def motioncorr(**kwargs):
        """Run motioncorr on the data files and xcom push all of the data"""
        pass
    t_motioncorr = PythonOperator(task_id='motioncorr',
        python_callable=motioncorr,
        op_kwargs={}
    )

    t_upload_motioncorr = DummyOperator(task_id='upload_motioncorr_to_logbook')


    def ctffind(**kwargs):
        """Run ctffind on the data files and xcom push all of the data"""
        pass
    t_ctffind = PythonOperator(task_id='ctffind',
        python_callable=ctffind,
        op_kwargs={}
    )


    t_upload_ctffind = DummyOperator(task_id='upload_ctffind_to_logbook')


    t_clean = DummyOperator(task_id='clean_up')



    ###
    # define pipeline
    ###


    t_parameters >> t_logbook >> t_clean

    t_tif2mrc >> t_motioncorr >> t_upload_motioncorr >> t_clean

    t_tif2mrc >> t_ctffind >> t_upload_ctffind >> t_clean

