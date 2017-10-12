from datetime import datetime

from airflow import utils
from airflow import DAG

from airflow.hooks.base_hook import BaseHook

from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.hooks.databricks_hook import DatabricksHook

from airflow.exceptions import AirflowException

from airflow.utils.logging import LoggingMixin

import time

try:
     from pythonlsf import lsf
     lsf.lsb_init("airflow-lsf-hook")
except:
     print("could not load lsf")

import logging
LOG = logging.getLogger(__name__)


class Job:
    def __init__ ( self, command, queue=None, jobName=None, 
           log=None, priority=None, 
           numProc=1, resource=None, beginTime=0 ) :
        self.__reset()
        self.name = jobName
        self.priority = priority
        self.numProc = numProc
        self.queue = queue
        self.command = command
        self.resource = resource
        self.beginTime = beginTime
        
    def jobid(self) :
        return self._jobid

    def status(self):
        return self._status

    def statusStr(self):
        return _statStr(self._status)

    def exitStatus(self):
        return self._exitStatus

    def __str__(self):
        return "Job(id=%s,status=%s)" % (lsf.lsb_jobid2str(self._jobid), _statStr(self._status))

    def submit(self):
        req = lsf.submit()
        options, options2 = 0, 0
        req.command = self.command
        if self.queue:
            options = options | lsf.SUB_QUEUE
            req.queue = self.queue
        if self.name:
            options = options | lsf.SUB_JOB_NAME
            req.jobName = self.name
        if resource:
            options = options | lsf.SUB_RES_REQ
            req.resReq = self.resource
        if log:
            options = options | lsf.SUB_OUT_FILE
            options2 = options2 | lsf.SUB2_OVERWRITE_OUT_FILE
            req.outFile = log
        if priority is not None:
            options2 = options2 | lsf.SUB2_JOB_PRIORITY
            req.userPriority = self.priority
        
        req.options = options
        req.options2 = options2

        req.rLimits = [lsf.DEFAULT_RLIMIT] * lsf.LSF_RLIM_NLIMITS

        req.beginTime = self.beginTime
        req.termTime = 0
        req.numProcessors = self.numProc
        req.maxNumProcessors = self.numProc

        reply = lsf.submitReply()

        self.__jobid = lsf.lsb_submit(req, reply)
        if self.__jobid < 0: 
            raise LSBError()
        return self
        

    def update(self):
        data = self.__update()
        if data:
            # update status
            self.__set(data)

    def setPriority(self, priority):
        """Change priority of the existing job"""
        
        data = self.__update()
        if data is None:
            logging.warning('Job.setPriority() failed to get current status for %s', self)
            return

        req = lsf.submit()
        options, options2 = 0, 0
        req.command = str(self._jobid)
        if priority is not None:
            options2 = options2 | lsf.SUB2_JOB_PRIORITY
            req.userPriority = priority
            
        req.options = options
        req.options2 = options2
    
        req.rLimits = data.submit.rLimits
    
        req.beginTime = data.submit.beginTime
        req.termTime = 0
        req.numProcessors = data.submit.numProcessors
        req.maxNumProcessors = data.submit.maxNumProcessors
        
        reply = lsf.submitReply()
    
        self._jobid = lsf.lsb_modify(req, reply, self._jobid)
        if self._jobid < 0:
            raise LSBError()

        self.priority = priority

    def kill(self, sig=None):
        """Send signal to a job"""
        
        if sig is None:
            # this follows bkill algorithm
            lsf.lsb_signaljob(self._jobid, signal.SIGTERM)
            lsf.lsb_signaljob(self._jobid, signal.SIGINT)
            # give it 5 seconds to cleanup
            time.sleep(5)
            lsf.lsb_signaljob(self._jobid, signal.SIGKILL)
        else:
            lsf.lsb_signaljob(self._jobid, sig)
        
    def __update(self):
        """ Retrieves job status information from LSF and updates internal state."""
        count = lsf.lsb_openjobinfo(self._jobid, None, "all", None, None, lsf.ALL_JOB)
        if count < 1:
            logging.warning('lsb_openjobinfo() failed for %s (job may be finished long ago or has not started)', self)
            self.__reset()
            data = None
        else:
            if count > 1:
                logging.warning('lsb_openjobinfo() returned more than one match for %s', self)
            jobp = lsf.new_intp()
            lsf.intp_assign(jobp, 0)
            data = lsf.lsb_readjobinfo(jobp)

        lsf.lsb_closejobinfo()
        return data

    def __reset(self):
        self._status = None
        self._exitStatus = None
        self.priority = None
        self._user = None
        self._durationMinutes = None
        self._cpuTime = None
        self.name = None

    def __set(self, data):
        self._status = data.status
        self._exitStatus = data.exitStatus
        self.priority = data.jobPriority
        self._user = data.user
        self._durationMinutes = data.duration
        self._cpuTime = data.cpuTime
        self.name = data.jName




class LsfHook(BaseHook, LoggingMixin):
    """
    Interact with LSF
    """
    def __init__(
        self,
        job=None,
        lsf_conn_id='lsf_default',
        timeout_seconds=180,
        retry_limit=3):
        self.lsf_conn_id = lsf_conn_id
        self.timeout_seconds = timeout_seconds
        assert retry_limit >= 1, 'Retry limit must be greater than equal to 1'
        self.retry_limit = retry_limit
        self.job = None

    def submit_run(self, command, queue=None, jobName=None, log=None, priority=None, numProc=1, resource=None, beginTime=0):
        job = Job(command, queue=queue, jobName=jobName, log=log, priority=priority, numProc=numProc, resource=resource, beginTime=beginTime)
        job.submit()
        return job

    def get_run_state(self, job):
        return RunState(job)

RUN_LIFE_CYCLE_STATES = [
    'PEND',
    'PROV',
    'PSUSP', # suspended
    'RUN',
    'USUSP', # suspended
    'SSUSP', # suspended
    'DONE',
    'EXIT',
    'UNKWN',
    'WAIT',
    'ZOMBI',
]

class RunState:
    """
    Utility class to to map lsf run states
    """
    def __init__(self, job):
        self.job = job
    @property
    def is_terminal(self):
        return self.job.state() in ( 'DONE', 'EXIT', 'ZOMBI', 'UNKWN' )
    @property
    def is_successful(self):
        return self.job.state() in ( 'DONE', )


class LsfSubmitRunOperator(BaseOperator):
    """
    Submits a job to LSF
    """
    def __init__(
        self,
        timeout_seconds=None,
        polling_period_seconds=30 ):
        """Creates a new ``LsfSubmitRunOperator``"""
        super(LsfSubmitRunOperator,self).__init__(**kwargs)
        self.job = {}

    def get_hook(self):
        return LsfHook(job=self.job)

    def execute(self, context):
        hook = self.get_hook()
        self.run_id = hook.submit_run(self.job)
        LOG.info('LSF job %s submitted with job id %s' % (self.job,self.run_id))
        while True:
            run_state = hook.get_run_state(self.job)
            if run_state.is_terminal:
                if run_state.is_successful:
                    LOG.info('{} completed successfully').format(self.task_id)
                    return
                else:
                    raise AirflowException('{t} failed with terminal state: {s}'.format(t=self.task_id,s=run_state))
            else:
                LOG.info('{t} in run state: {s}'.format(t=self.task_id,s=run_state))
                LOG.info('sleeping for {} seconds'.format(self.polling_period_seconds))
                time.sleep(self.polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        LOG.info('Task: {t} with run_id: {r} was requested to be cancelled'.format(t=self.task_id,r=self.run_id))

        
if __name__ == "__main__":
    LOG.info("testing lsf submission")
    job = Job('date')
    job.submit()
    
