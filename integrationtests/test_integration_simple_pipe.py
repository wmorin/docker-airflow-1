from airflow.api.common.experimental import trigger_dag as trigger
from airflow.api.common.experimental import delete_dag as delete
from airflow.api.common.experimental.get_dag_run_state import get_dag_run_state
from airflow.utils import timezone
from airflow.exceptions import AirflowException
from airflow.utils.db import provide_session
from airflow import models

import pytest
import json
import os

class TestIntegrationSimplePipe:

    def pause_dag(self, dag_id, pause):
        """(Un)pauses a dag"""
        models.DagModel.get_dagmodel(dag_id).set_is_paused(
            is_paused=pause
        )
        
    @provide_session
    def clean_dag(self, dag_id, session=None):
        """
        Delete all DB records related to the specified Dag.
        """
        tables = [
            models.DagRun,
            models.TaskInstance,
            models.log.Log,
            models.taskfail.TaskFail,
            models.taskreschedule.TaskReschedule,
        ]
        
        for table in tables:
            session.query(table).filter(table.dag_id == dag_id).delete()
        
    def trigger_dag(self, dag_id, execution_date):
        """
        Trigger a new dag run for a Dag with an execution date.
        """        
        execution_date = timezone.parse(execution_date)
        trigger.trigger_dag(dag_id, None, None, execution_date)

    def status_dag(self, dag_id, execution_date):
        """
        Get the status of a given DagRun according to the execution date
        """
        execution_date = timezone.parse(execution_date)
        info = get_dag_run_state(dag_id, execution_date)
        return info
        
    def test_simple_pipe(self):
        """ Simple Pipe should run successfully """
        execution_date = "2020-05-21T12:00:00+00:00"
        dag_id = "simple_pipe"

        # Delete data related to the DAG (if already exists)
        self.clean_dag(dag_id)
        
        # Unpause DAG - Required to trigger it, even manually
        self.pause_dag(dag_id, False)

        # Trigger the DAG
        self.trigger_dag(dag_id, execution_date)
        
        # Check if it is running as expected
        is_running = True
        while is_running:
            dagrun = self.status_dag(dag_id, execution_date)['state']
            if dagrun not in ['running', 'success']:
                return
            if dagrun == 'success':
                is_running = False
        assert is_running == False, "The DAG {} didn't run as expected".format(dag_id)

        # pause DAG
        self.pause_dag(dag_id, True)