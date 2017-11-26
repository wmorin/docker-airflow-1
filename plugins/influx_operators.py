
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from collections import defaultdict, MutableMapping
from ast import literal_eval
from datetime import datetime, timedelta
from dateutil import parser, tz
import pytz
import re

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

def dummy(*args,**kwargs):
    pass

class InfluxOperator(PythonOperator):
    ui_color = '#4bcf9a'
    template_fields = ('experiment',)
    def __init__(self,host='localhost',port=8086,user='root',password='root',db='cryoem',measurement='microscope_image',experiment=None,*args,**kwargs):
        super(InfluxOperator,self).__init__(python_callable=dummy,*args,**kwargs)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.measurement = measurement
        self.experiment = experiment
    def execute(self, context):
        """Push the parameter key-value pairs to the elogbook"""
        about = {}
        data = {} 
        dt, about, data = self.process(context)
        client = influxdb.InfluxDBClient( self.host, self.port, self.user, self.password, self.db )
        #LOG.info("DB: %s:%s/%s (%s %s)" % (self.host, self.port, self.db, self.user, self.password) )
        #LOG.info( '%s @ %s: \n%s\n%s' % (self.measurement,dt.strftime('%s'),pformat(context),pformat(data) ) )
        client.create_database(self.measurement)
        if self.experiment:
           context['experiment'] = self.experiment
        LOG.info('writing datapoint at %s to %s' % (dt, self.measurement))
        client.write_points([{
            "measurement": self.measurement,
            "tags": about,
            "fields": data,
            "time": dt,
        }])
        return
    def process(self, context):
        return NotImplementedError('not implemented here')

class Xcom2InfluxOperator(InfluxOperator):
    def __init__(self,xcom_task_id=None,xcom_key='return_value',*args,**kwargs):
        super(Xcom2InfluxOperator,self).__init__(*args,**kwargs)
        self.xcom_task_id = xcom_task_id
        self.xcom_key = xcom_key

class FeiEpu2InfluxOperator(Xcom2InfluxOperator):
    def process(self, context):
        LOG.info("CONTEXT: %s" % (context,))
        d = context['ti'].xcom_pull( task_ids=self.xcom_task_id, key=self.xcom_key )['MicroscopeImage']
        dt = parser.parse( d['microscopeData']['acquisition']['acquisitionDateTime'] )
        dd = flatten(d, sep='_')
        about = {}
        data = {}
        for k,v in dd.items():
            # ignore these entries
            if k in ( 'microscopeData_acquisition_acquisitionDateTime', 'CustomData_FindFoilHoleCenterResults_@type' ):
                continue
            # force about
            elif k in ( 'microscopeData_instrument_InstrumentID', ):
                v = '%s' % v
            # LOG.info("k=%s, v=%s" % (k,v))
            if isinstance( v, (str,bool) ) or v == None:
                # LOG.info("  about")
                vv = "'%s'" % v if isinstance(v,str) and ' ' in v else v
                about[k] = vv
            else:
                # LOG.info("  data %s" % v)
                data[k] = float(v)
        return dt, about, data

class LSFJob2InfluxOperator(Xcom2InfluxOperator):
    def __init__(self, measurement='preprocessing', job_name='lsf', *args, **kwargs):
        super(LSFJob2InfluxOperator,self).__init__(*args,**kwargs)
        self.job_name = job_name
        self.measurement = measurement
    def process(self, context, tz="America/Los_Angeles"):
        d = context['ti'].xcom_pull( task_ids=self.xcom_task_id, key=self.xcom_key )
        about = {
            'job_name': self.job_name,
            'experiment': self.experiment,
            'host': d['host'],
        }
        # use more accurate duration if available
        runtime = d['runtime'].total_seconds()
        m = re.search( '(<seconds>\d+\.\d+) seconds', d['duration'] )
        if m:
            d = m.groupdict()
            runtime = d['seconds']
        data = {
            'inertia': d['inertia'].total_seconds(),
            'runtime': runtime,
            # 'duration': d['duration'],
        }
        # convert to UTC
        def is_dst(tz):
            now = pytz.utc.localize(datetime.utcnow())
            return now.astimezone(tz).dst() != timedelta(0)
        host_tz = pytz.timezone( tz )
        dt = host_tz.normalize( host_tz.localize( d['submitted_at'], is_dst=is_dst(host_tz) ) ).astimezone( pytz.utc )
        return dt, about, data

class InfluxPlugin(AirflowPlugin):
    name = 'influx_plugin'
    operators = [InfluxOperator,FeiEpu2InfluxOperator,LSFJob2InfluxOperator]

