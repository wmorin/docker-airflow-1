from airflow.plugins_manager import AirflowPlugin

from airflow.contrib.hooks import SSHHook

from airflow.models import BaseOperator
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator, SSHTempFileContent

from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.utils.decorators import apply_defaults

from airflow import utils

from builtins import bytes
import subprocess
from subprocess import STDOUT
import re
from datetime import datetime
from time import sleep
import dateutil

import logging

LOG = logging.getLogger(__name__)


DEFAULT_BSUB='/afs/slac/package/lsf/curr/bin/bsub'
DEFAULT_BJOBS='/afs/slac/package/lsf/curr/bin/bjobs'
DEFAULT_BKILL='/afs/slac/package/lsf/curr/bin/bkill'
DEFAULT_QUEUE_NAME='cryoem-daq'


class BaseSSHOperator(SSHExecuteOperator):
    template_fields = ("bash_command", "env",)
    template_ext = (".sh", ".bash",)
    
    def get_bash_command(self,context):
        return self.bash_command
    
    def execute(self, context):
        bash_command = self.get_bash_command(context)
        host = self.hook._host_ref()

        self.out = None

        with SSHTempFileContent(self.hook,
                                bash_command,
                                self.task_id) as remote_file_path:

            # note shell=True may need security parsing
            self.sp = self.hook.Popen(
                ['-q', 'bash', remote_file_path],
                stdout=subprocess.PIPE, stderr=STDOUT,
                env=self.env)

            self.parse_output(context,self.sp)

            self.sp.wait()
            logging.info("Command exited with "
                         "return code {0}".format(self.sp.returncode))
            if self.sp.returncode:
                raise AirflowException("Bash command failed")

        if self.out:
            return self.out

    def parse_output(self,context,sp):
        logging.info("Output:")
        for line in iter(sp.stdout.readline, b''):
            line = line.decode().strip()
            logging.info(line)

        

class LSFSubmitOperator(BaseSSHOperator):
    """ Submit a job asynchronously into LSF and return the jobid via xcom return_value """
    template_fields = ("lsf_script", "env",)
    template_ext = (".sh", ".bash",)

    ui_color = '#0088aa'

    @apply_defaults
    def __init__(self,
                 ssh_hook,
                 lsf_script,
                 bsub=DEFAULT_BSUB,
                 queue_name=DEFAULT_QUEUE_NAME,
                 *args, **kwargs):
        self.bsub = bsub
        self.queue_name = queue_name
        self.lsf_script = lsf_script
        self.hook = ssh_hook
        self.bash_command = self.get_bash_command
        super(LSFSubmitOperator, self).__init__(ssh_hook=self.hook, bash_command=self.bash_command, *args, **kwargs)

    def get_bash_command(self, context):
        name = context['task_instance_key_str']
        return self.bsub + ' -cwd "/tmp" -q %s ' % self.queue_name + " -J %s" % name + " <<-'__LSF_EOF__'\n" + \
            self.lsf_script + "\n" + '__LSF_EOF__\n'    
    
    def parse_output(self,context,sp):
        logging.info("LSF Submit Output:")
        for line in iter(sp.stdout.readline, b''):
            line = line.decode().strip()
            logging.info(line)
            m = re.search( r'^Job \<(?P<jobid>\d+)\> is submitted to queue', line )
            if m:
                d = m.groupdict()
                self.out = d



class BaseSSHSensor(BaseSSHOperator):
    """ sensor via executing an ssh command """
    def __init__(self,
                 ssh_hook,
                 bash_command,
                 xcom_push=False,
                 poke_interval=10,
                 timeout=60*60,
                 soft_fail=False,
                 env=None,
                 *args, **kwargs):
        super(SSHExecuteOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.hook = ssh_hook
        self.xcom_push = xcom_push
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self.prevent_returncode = None

    def execute(self, context, bash_command_function='get_bash_command'):
        func = getattr(self, bash_command_function)
        bash_command = func(context)
        host = self.hook._host_ref()
        started_at = datetime.now()
        with SSHTempFileContent(self.hook,
                                bash_command,
                                self.task_id) as remote_file_path:
            logging.info("Temporary script "
                         "location : {0}:{1}".format(host, remote_file_path))

            while not self.poke_output(self.hook, context, remote_file_path):
                if (datetime.now() - started_at).total_seconds() > self.timeout:
                    if self.soft_fail:
                        raise AirflowSkipException('Snap. Time is OUT.')
                    else:
                        raise AirflowSensorTimeout('Snap. Time is OUT.')
                sleep(self.poke_interval)
            logging.info("Success criteria met. Exiting.")
        
            
    def poke_output(self, hook, context, remote_file_path):

        sp = hook.Popen(
            ['-q', 'bash', remote_file_path],
            stdout=subprocess.PIPE, stderr=STDOUT,
            env=self.env)
        self.sp = sp

        result = self.poke(context,sp)

        sp.wait()
        logging.info("Command exited with "
                     "return code {0}".format(sp.returncode))
        # LOG.info("PREVENT RETURNCODE: %s" % (self.prevent_returncode,))
        if sp.returncode and not self.prevent_returncode:
            raise AirflowException("Bash command failed: %s" % (sp.returncode,))
            
        return result

    def poke( self, context, sp ):
        raise AirflowException('Override me.')
        

class LSFJobSensor(BaseSSHSensor):
    """ waits for a the given lsf job to complete and supply results via xcom """
    template_fields = ("jobid",)
    ui_color = '#006699'

    def __init__(self,
                 ssh_hook,
                 jobid,
                 bjobs=DEFAULT_BJOBS,
                 *args, **kwargs):
        self.hook = ssh_hook
        self.jobid = jobid
        self.bjobs = bjobs
        self.bash_command = self.get_bash_command
        self.prevent_returncode = None
        super(LSFJobSensor, self).__init__(ssh_hook=self.hook,bash_command=self.bash_command,*args, **kwargs)

    def get_bash_command(self, context):
        return self.bjobs + ' -l ' + self.jobid

    def poke( self, context, sp ):
        LOG.info('Querying LSF job %s' % (self.jobid,))
        info = {}
        for line in iter(sp.stdout.readline, b''):
            line = line.decode().strip()
            LOG.info(line)
            if ' Status <DONE>, ' in line:
                info['status'] = 'DONE'
            elif ' Status <EXIT>, ' in line:
                info['status'] = 'EXIT'
            elif ' Status <PEND>, ' in line:
                info['status'] = 'PEND'
            elif ' Submitted from host' in line:
                dt, _ = line.split(': ')
                info['submitted_at'] = dateutil.parser.parse( dt )
            elif ' Started on ' in line:
                m = re.search( '^(?P<dt>.*): Started on \<(?P<host>.*)\>, Execution Home ', line )
                if m:
                    d = m.groupdict()
                    info['started_at'] = dateutil.parser.parse( d['dt'] )
                    info['host'] = d['host']
            elif ' The CPU time used is ' in line:
                m = re.search( '^(?P<dt>.*)\: .*\. The CPU time used is (?P<duration>.*)\.', line )
                if m:
                    d = m.groupdict()
                    info['finished_at']= dateutil.parser.parse( d['dt'])
                    info['duration'] = d['duration']
    
            # upstream failure
            elif 'Dependency condition invalid or never satisfied' in line:
                # run bkill to remove job from queue and report back error
                with SSHTempFileContent(self.hook,
                                        "%s %s" % (DEFAULT_BKILL, self.jobid),
                                        self.task_id + '_kill') as remote_file_path:
                    killsp = self.hook.Popen(
                        ['-q', 'bash', remote_file_path],
                        stdout=subprocess.PIPE, stderr=STDOUT,
                        env=self.env)
                    okay = False
                    for l in iter(killsp.stdout.readline, b''):
                        LOG.info("BKILL %s" % l)
                        if b'is being terminated' in l:
                            okay = True
                    killsp.wait()
                    if killsp.returncode or not okay:
                        raise AirflowException("Could not kill job %s" % self.jobid)
                    
                raise AirflowException('Job dependency condition invalid or never satisfied')
                
    
        LOG.info(" %s" % (info,))

        if 'status' in info:
            
            # stupid bjobs may sometimes exit even tho the job exists 
            # so if we've previously found the job, don't get bjobs exit
            self.prevent_returncode = True
            if 'submitted_at' and 'started_at' in info:
                info['inertia'] = info['started_at'] - info['submitted_at']
            if 'finished_at' and 'started_at' in info:
                info['runtime'] = info['finished_at'] - info['started_at']
        
            if info['status'] == 'DONE':
                context['ti'].xcom_push( key='return_value', value=info )
                return True
            elif info['status'] == 'EXIT':
                # TODO: bpeek? write std/stderr?
                context['ti'].xcom_push( key='return_value', value=info )
                raise AirflowException('Job EXITed')
    
        return False


class LSFOperator(LSFSubmitOperator,LSFJobSensor):
    """ Submit a job into LSF wait for the job to finish """
    template_fields = ("lsf_script", "env",)
    template_ext = (".sh", ".bash",)

    ui_color = '#006699'

    @apply_defaults
    def __init__(self,
                 ssh_hook,
                 lsf_script,
                 bsub=DEFAULT_BSUB,
                 bjobs=DEFAULT_BJOBS,
                 queue_name=DEFAULT_QUEUE_NAME,
                 poke_interval=10,
                 timeout=60*60,
                 soft_fail=False,
                 env=None,
                 *args, **kwargs):
        self.bsub = bsub
        self.bjobs = bjobs
        self.queue_name = queue_name
        self.lsf_script = lsf_script
        self.hook = ssh_hook
        self.jobid = None
        self.timeout = timeout
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.env = env
        self.prevent_returncode = None
        BaseOperator.__init__( self, *args, **kwargs)

    def get_status_command(self, context):
        return LSFJobSensor.get_bash_command(self,context)
    
    def execute(self, context):
        hook = self.hook
        host = hook._host_ref()
        
        LSFSubmitOperator.execute(self, context)
        
        if self.out:
            self.jobid = self.out['jobid']
        if not self.jobid:
            raise AirflowException("Could not determine jobid")
        
        LSFJobSensor.execute(self, context, bash_command_function='get_status_command')


        

class LSFPlugin(AirflowPlugin):
    name = 'lsf_plugin'
    operators = [LSFSubmitOperator,LSFJobSensor,LSFOperator]
