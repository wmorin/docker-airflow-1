from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator

from airflow.operators.python_operator import ShortCircuitOperator

from builtins import bytes

from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory

import ast
import glob
import shutil
import os
import shutil
from pathlib import Path

import logging

LOG = logging.getLogger(__name__)

class FileGlobSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, directory, pattern, recursive, *args, **kwargs):
        super(FileGlobSensor, self).__init__(*args, **kwargs)
        self.dirpath = directory
        self.globpattern = pattern
        self.recursive = recursive
    def poke(self, context):
        files = []
        os.chdir( self.dirpath )
        for f in glob.iglob( self.globpattern, recursive=self.recursive ):
            files.append(f) 
        LOG.info('found files: %s' % (files) )
        if len(files):
            context['task_instance'].xcom_push(key='files',value=files)
            # file = files.pop(0)
            # context['task_instance'].xcom_push(key='filepath',value=file)
            # context['task_instance'].xcom_push(key='directory',value=os.path.dirname(file))
            return True
        return False
def fileGlob(**kwargs):
    v = '%s/%s' % (kwargs['directory'], kwargs['pattern'])
    LOG.info('Checking for files with %s' % (v,))
    recursive = False
    if 'recursive' in kwargs:
        recursive = kwargs['recursive']
    for f in glob.iglob( v, recursive=recursive ):
        LOG.info(" found file %s" % (f,))
        kwargs['task_instance'].xcom_push(key='filepath',value=f)
        return f
    return False
class FileGlobExistsOperator(ShortCircuitOperator):
    """ will skip downstream tasks if the file doesn't exist """
    def __init__(self, directory, pattern, recursive, *args, **kwargs):
        super(FileGlobExistsOperator, self).__init__(python_callable=fileGlob, op_kwargs={'directory': directory, 'pattern': pattern, 'recursive': recursive}, *args, **kwargs)



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


class RsyncOperator(BaseOperator):
    """
    Execute a rsync
    """
    template_fields = ('env','source','target','excludes','includes')
    template_ext = ( '.sh', '.bash' )
    ui_color = '#f0ede4'
    


    @apply_defaults
    def __init__(self, source, target, xcom_push=True, env=None, output_encoding='utf-8', prune_empty_dirs=False, includes='', excludes='', flatten=False, dry_run=False, *args, **kwargs ):
        super(RsyncOperator, self).__init__(*args,**kwargs)
        self.env = env
        self.output_encoding = output_encoding
        
        self.source = source
        self.target = target
        
        self.includes = includes
        self.excludes = excludes
        self.prune_empty_dirs = prune_empty_dirs
        self.flatten = flatten
        self.dry_run = dry_run
        
        self.xcom_push_flag = xcom_push
        
        self.rsync_command = ''
        
    def execute(self, context):
                
        output = []
        # LOG.info("tmp dir root location: " + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                includes = ''
                try:
                    a = self.includes
                    if isinstance(self.includes, str):
                        a = ast.literal_eval(self.includes)
                    inc = [ "-name '%s'" % i for i in a ]
                    includes = ' -o '.join(inc)
                except:
                    if self.includes:
                        includes = " -name '%s'" % (self.includes,)

                # format rsync command
                rsync_command = "find %s -type f \( %s \) | rsync -av %s --files-from - %s %s %s %s" % ( \
                        self.source,
                        includes,
                        '--dry-run' if self.dry_run else '', \
                        '-d --no-relative' if self.flatten else '', \
                        '--prune-empty-dirs' if self.prune_empty_dirs else '', \
                        '/',
                        self.target )


                f.write(bytes(rsync_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))
                logging.info("Running rsync command: " + rsync_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env)

                self.sp = sp

                logging.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    LOG.info(line)
                    # parse for file names here
                    if line.startswith( 'building file list' ) or line.startswith( 'sent ') or line.startswith( 'total size is ' ) or line in ('', './'):
                        continue
                    else:
                        output.append( line )
                sp.wait()
                logging.info("Command exited with "
                             "return code {0}".format(sp.returncode))

                if sp.returncode:
                    raise AirflowException("rsync command failed")

        if self.xcom_push_flag:
            return output

        
    def on_kill(self):
        LOG.info('Sending SIGTERM signal to bash subprocess')
        self.sp.terminate()




class FilePlugin(AirflowPlugin):
    name = 'file_plugin'
    operators = [FileGlobSensor,FileGlobExistsOperator,EnsureDirectoryExistsOperator,FileOperator,RsyncOperator]
