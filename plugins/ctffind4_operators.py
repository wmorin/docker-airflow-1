from airflow.operators.sensors import BaseSensorOperator

from airflow.plugins_manager import AirflowPlugin

import os
import glob

import logging
LOG = logging.getLogger(__name__)


class Ctffind4DataSensor(BaseSensorOperator):
    ui_color = '#9ACD32'
    template_fields = ('filepath',)
    # micrograph number; #2 - defocus 1 [Angstroms]; #3 - defocus 2; #4 - azimuth of astigmatism; #5 - additional phase shift [radians]; #6 - cross correlation; #7 - spacing (in Angstroms) up to which CTF rings were fit successfully
    ctffind_fields = ( 'micrograph', 'defocus_1', 'defocus_2', 'cs', 'additional_phase_shift', 'cross_correlation', 'resolution')
    
    def __init__(self,filepath=None,recursive=False,*args,**kwargs):
        super(Ctffind4DataSensor,self).__init__(*args,**kwargs)
        self.filepath = filepath
        self.recursive = recursive

    def poke(self, context):
        LOG.info('Waiting for file %s' % (self.filepath,) )
        for this in glob.iglob( self.filepath, recursive=self.recursive ):
            # LOG.warn("FILEPATH: %s" % self.filepath)
            tags = {}
            data = {}
            with open(this) as f:
                for l in f.readlines():
                    if l.startswith('# Pixel size:') or l.startswith('# Box'):
                        for a in l.split(';'):
                            k = None
                            v = None
                            try:
                                k,v = a.split(':')
                                k = k.replace('#','').strip().lower().replace('.','').replace(' ','_')
                                v = v.strip()
                            except:
                                if a.startswith(' max. def. '):
                                     k = 'max_def'
                                     v = a.replace(' max. def. ','')
                            try:
                                stuff = v.split(' ')
                            except:
                                stuff = [ v ]
                            # LOG.info('%s, %s' % (k, stuff))
                            tags[k] = float(stuff[0])
                    elif l.startswith('#'):
                        continue
                    else:
                        a = l.split()
                        # LOG.warn(" LINE: %s" % (a,))
                        for n,value in enumerate(a):
                            data[ self.ctffind_fields[n] ] = float(value)
                        # LOG.warn(" DATA: %s" % (data,))

            data['resolution_performance'] = 2 * tags['pixel_size'] / data['resolution']

            context['task_instance'].xcom_push(key='return_value',value=data)
            context['task_instance'].xcom_push(key='context',value=tags)
            return True
        LOG.error("Could not find file %s" % (self.filepath,))
        return False
        
        

class Ctffind4Plugin(AirflowPlugin):
    name = 'ctffind4_plugin'
    operators = [Ctffind4DataSensor,]
