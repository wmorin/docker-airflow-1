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

import logging
import time

LOG = logging.getLogger(__name__)


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
        
    def submit_run(self, job):
        # create job
        pass
        
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