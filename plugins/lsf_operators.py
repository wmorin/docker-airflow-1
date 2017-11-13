from airflow.plugins_manager import AirflowPlugin

from airflow.contrib.hooks import SSHHook
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator, SSHTempFileContent

from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from airflow import utils

from builtins import bytes
import subprocess
from subprocess import STDOUT
import re
from datetime import datetime
from time import sleep

import logging


class BaseSSHOperator(SSHExecuteOperator):
    template_fields = ("bash_command", "env",)
    template_ext = (".sh", ".bash",)
    
    def get_bash_command(self,context):
        return self.bash_command
    
    def execute(self, context):
        bash_command = self.get_bash_command(context)
        hook = self.hook
        host = hook._host_ref()

        self.out = None

        with SSHTempFileContent(self.hook,
                                bash_command,
                                self.task_id) as remote_file_path:
            logging.info("Temporary script "
                         "location : {0}:{1}".format(host, remote_file_path))
            logging.info("Running command: " + bash_command)

            # note shell=True may need security parsing
            sp = hook.Popen(
                ['-q', 'bash', remote_file_path],
                # ['-q', 'bash', '-l', '-c', remote_file_path],
                # '-q bash -l -c %s' % (remote_file_path,),
                stdout=subprocess.PIPE, stderr=STDOUT,
                env=self.env)

            self.sp = sp

            self.parse_output(context,sp)

            sp.wait()
            logging.info("Command exited with "
                         "return code {0}".format(sp.returncode))
            if sp.returncode:
                raise AirflowException("Bash command failed")

        if self.out:
            return self.out

    def parse_output(self,context,sp):
        logging.info("Output:")
        for line in iter(sp.stdout.readline, b''):
            line = line.decode().strip()
            logging.info(line)
        

class LSFSubmitOperator(BaseSSHOperator):
    """ Submit a job into LSF and return the jobid via xcom return_value """
    template_fields = ("lsf_script", "env",)
    template_ext = (".sh", ".bash",)

    ui_color = '#006699'

    @apply_defaults
    def __init__(self,
                 ssh_hook,
                 lsf_script,
                 bsub='bsub',
                 queue_name='short',
                 *args, **kwargs):
        self.bsub = bsub
        self.queue_name = queue_name
        self.lsf_script = lsf_script
        self.hook = ssh_hook
        self.bash_command = self.get_bash_command
        super(LSFSubmitOperator, self).__init__(ssh_hook=self.hook, bash_command=self.bash_command, *args, **kwargs)

    def get_bash_command(self, context):
        name = 'test'
        return self.bsub + ' -q %s ' % self.queue_name + " -J %s" % name + " <<-'__LSF_EOF__'\n" + \
            self.lsf_script + "\n" + '__LSF_EOF__\n'    
    
    def parse_output(self,context,sp):
        logging.info("LSF Submit Output:")
        for line in iter(sp.stdout.readline, b''):
            line = line.decode().strip()
            logging.info(line)
            m = re.search( r'^Job \<(?P<jobid>\d+)\> is submitted to queue', line )
            if m:
                d = m.groupdict()
                self.out = d['jobid']

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

    def execute(self, context):
        bash_command = self.get_bash_command(context)
        hook = self.hook
        host = hook._host_ref()
        self.out = None
        
        started_at = datetime.now()

        with SSHTempFileContent(self.hook,
                                bash_command,
                                self.task_id) as remote_file_path:
            logging.info("Temporary script "
                         "location : {0}:{1}".format(host, remote_file_path))
            logging.info("Running command: " + bash_command)

            while not self.poke_output(hook, context, remote_file_path):
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
        if sp.returncode:
            raise AirflowException("Bash command failed")
            
        return result

    def poke( self, context, sp ):
        raise AirflowException('Override me.')
        

class LSFJobSensor(BaseSSHSensor):
    template_fields = ("jobid",)
    ui_color = '#006699'

    def __init__(self,
                 ssh_hook,
                 jobid,
                 bjobs='bjobs',
                 *args, **kwargs):
        self.hook = ssh_hook
        self.jobid = jobid
        self.bjobs = bjobs
        self.bash_command = self.get_bash_command
        super(LSFJobSensor, self).__init__(ssh_hook=self.hook,bash_command=self.bash_command,*args, **kwargs)

    def get_bash_command(self, context):
        return self.bjobs + ' ' + self.jobid

    def poke( self, context, sp ):
        logging.info("\nLSF Sensor Output:")
        for line in iter(sp.stdout.readline, b''):
            line = line.decode().strip()
            logging.info(line)
            if ' DONE ' in line:
                return True
            elif ' EXIT ' in line:
                raise AirflowException('Job EXITed')
        return False
        

class LSFPlugin(AirflowPlugin):
    name = 'ssh_plugin'
    operators = [LSFSubmitOperator,LSFJobSensor]
