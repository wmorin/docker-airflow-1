
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from collections import defaultdict, MutableMapping
from ast import literal_eval
from dateutil import parser

import influxdb

import logging
LOG = logging.getLogger(__name__)



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




class InfluxPlugin(AirflowPlugin):
    name = 'influx_plugin'
    operators = [Send2InfluxOperator,]

