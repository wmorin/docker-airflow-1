import glob

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator

class FileGlobSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, dirpath, pattern, *args, **kwargs):
        super(FileGlobSensor, self).__init__(*args, **kwargs)
        self.dirpath = dirpath
        self.globpattern = pattern

    def poke(self, context):
        for f in glob.glob( self.dirpath + '/' + self.globpattern ):
            context['task_instance'].xcom_push('file_path',f)
            return True


class FilePlugin(AirflowPlugin):
    name = 'file_plugin'
    operators = [FileGlobSensor]
