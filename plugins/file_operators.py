from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.operators.sensors import BaseSensorOperator
from airflow.models import BaseOperator
from airflow.operators.python_operator import ShortCircuitOperator

import glob
import shutil
import os
import shutil
from pathlib import Path

import logging

LOG = logging.getLogger(__name__)

class FileGlobSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, directory, pattern, *args, **kwargs):
        super(FileGlobSensor, self).__init__(*args, **kwargs)
        self.dirpath = directory
        self.globpattern = pattern
    def poke(self, context):
        for f in glob.glob( self.dirpath + '/' + self.globpattern ):
            context['task_instance'].xcom_push(key='filepath',value=f)
            return True

def fileGlob(**kwargs):
    v = '%s/%s' % (kwargs['directory'], kwargs['pattern'])
    LOG.info('Checking for files with %s' % (v,))
    for f in glob.glob( v ):
        LOG.info(" found file %s" % (f,))
        kwargs['task_instance'].xcom_push(key='filepath',value=f)
        return f
    return False
class FileGlobExistsOperator(ShortCircuitOperator):
    """ will skip downstream tasks if the file doesn't exist """
    def __init__(self, directory, pattern, *args, **kwargs):
        super(FileGlobExistsOperator, self).__init__(python_callable=fileGlob, op_kwargs={'directory': directory, 'pattern': pattern}, *args, **kwargs)



def ensureDirectoryExists(**kwargs):
    LOG.info("Checking directory %s" % (kwargs['directory'],))
    if not os.path.exists(kwargs['directory']):
        try:
            os.makedirs(kwargs['directory'])
        except Exception as e:
            raise AirflowException('Error creating destination directory: %s' % (e,))
    return kwargs['directory']
class EnsureDirectoryExistsOperator(ShortCircuitOperator):
    """ will create directories specified if it doesn't already exist """
    def __init__(self,directory,*args,**kwargs):
        super(EnsureDirectoryExistsOperator,self).__init__(python_callable=ensureDirectoryExists, op_kwargs={'directory': directory}, *args, **kwargs)


class FileOperator(BaseOperator):
    @apply_defaults
    def __init__(self,source,destination, *args, **kwargs):
        super(FileOperator, self).__init__(*args,**kwargs)
        self.src = source
        self.dst = destination
    def execute(self, context):
        self.log.info('Moving file from %s to %s' % (self.src, self.dst))
        try:
            shutil.move( self.src, self.dst )
            return self.dst
        except Exception as e:
            raise AirflowException('Error moving file: %s' % e)






class FilePlugin(AirflowPlugin):
    name = 'file_plugin'
    operators = [FileGlobSensor,FileGlobExistsOperator,EnsureDirectoryExistsOperator,FileOperator]
