from airflow.operators.sensors import BaseSensorOperator

from airflow.plugins_manager import AirflowPlugin

import os
import glob
import re
import math

import logging
LOG = logging.getLogger(__name__)


class MotionCor2DataSensor(BaseSensorOperator):
    ui_color = '#9ACD32'
    template_fields = ('filepath',)
    
    def __init__(self,filepath=None,recursive=False,*args,**kwargs):
        super(MotionCor2DataSensor,self).__init__(*args,**kwargs)
        self.filepath = filepath
        self.recursive = recursive

    def poke(self, context):
        LOG.info('Waiting for file %s' % (self.filepath,) )
        for this in glob.iglob( self.filepath, recursive=self.recursive ):
            # LOG.warn("FILEPATH: %s" % self.filepath)
            tags = {}
            data = {}
            xy = []

            with open( this, 'r') as align:
                for l in align.readlines():
                    # print(" L: %s" % (l,))
                    m = re.match(r'^\s+(?P<i>\d+)\s+(?P<x>.*\d+\.\d+)\s+(?P<y>.*\d+\.\d+)\s+$', l)
                    if m:
                        d = m.groupdict()
                        # print( pformat(d) )
                        xy.insert( int(d['i'])-1, ( float(d['x']), float(d['y']) ) )

            # print( "XY: %s" % (xy,))
            data['frames'] = len(xy)
            total = 0
            for i, coord in enumerate(xy):
                if i < data['frames']-1:
                    # print( "%s: %s -> %s" % (i,coord,xy[i+1]))
                    dx2 = ( xy[i+1][0] - coord[0] )**2
                    dy2 = ( xy[i+1][1] - coord[1] )**2
                    sigma = math.sqrt( dx2 + dy2 )
                    # print( " %s, %s -> %s" % (dx2,dy2, sigma))
                    total = total + sigma
            drift = total / len(xy)

            data['drift'] = drift

            context['task_instance'].xcom_push(key='return_value',value=data)
            context['task_instance'].xcom_push(key='context',value=tags)
            return True
        LOG.error("Could not find file %s" % (self.filepath,))
        return False
        
        

class MotionCor2Plugin(AirflowPlugin):
    name = 'motioncor2_plugin'
    operators = [MotionCor2DataSensor,]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
