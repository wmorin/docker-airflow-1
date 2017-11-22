
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from collections import defaultdict, MutableMapping
import xml.etree.ElementTree as ET
from ast import literal_eval
from dateutil import parser

import logging
LOG = logging.getLogger(__name__)

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

def dummy(ds, **kwargs):
    pass

class FeiEpuOperator(PythonOperator):
    """ read the experimental parameters from an FEI xml file """
    template_fields = ('filepath',)
    def __init__(self,filepath,*args,**kwargs):
        super(FeiEpuOperator,self).__init__(python_callable=dummy,*args,**kwargs)
        # BaseOperator.__init__(self,*args,**kwargs)
        self.filepath = filepath
        LOG.info("XML Parse %s" % (self.filepath,))
    def execute(self, context):
        LOG.info('parsing fei epu xml file %s' % (self.filepath,))
        return etree_to_dict( ET.parse( self.filepath ).getroot() )


class FeiEpuPlugin(AirflowPlugin):
    name = 'fei_epu_plugin'
    operators = [FeiEpuOperator,]

