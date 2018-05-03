from airflow import DAG
import os
from datetime import datetime

from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators import GenericInfluxOperator, InfluxOperator #, Xcom2InfluxOperator
import influxdb

import logging
LOG = logging.getLogger(__name__)

class MyPostgresOperator(PostgresOperator):
    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        data = self.hook.get_records(self.sql, parameters=self.parameters)
        context['ti'].xcom_push( key='return_value', value=data )

class Xcom2InfluxOperator(InfluxOperator):
    def __init__(self,xcom_task_id=None,xcom_key='return_value',*args,**kwargs):
        super(Xcom2InfluxOperator,self).__init__(*args,**kwargs)
        self.xcom_task_id = xcom_task_id
        self.xcom_key = xcom_key

class MyInfluxOperator(Xcom2InfluxOperator):
    def execute(self, context):
        """Push the parameter key-value pairs to the elogbook"""
        d = context['ti'].xcom_pull( task_ids=self.xcom_task_id, key=self.xcom_key )
        dag = {}
        for l in d:
            dag_id = l[0]
            if dag_id[0].isdigit():
                if not dag_id in dag:
                    dag[dag_id] = {}
                dag[dag_id][l[1]] = l[2]

        client = influxdb.InfluxDBClient( self.host, self.port, self.user, self.password, self.db )
        client.create_database(self.measurement)
        #LOG.info("DAG: %s" % (dag,))
        for k,data in dag.items():
            about = { 'dag_id': k }
            for s in ( 'failed', 'success', 'running', ):
                if not s in data:
                    data[s] = 0
            LOG.info('writing datapoint to %s: tag %s fields %s' % (self.measurement, about, data))
            client.write_points([{
                "measurement": self.measurement,
                "tags": about,
                "fields": data,
            }])
        return

with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Report airflow DAG states to influxdb",
        schedule_interval="* * * * *",
        catchup=False,
        start_date=datetime(2018,1,1),
        max_active_runs=1
    ) as dag:

    dag_stats = MyPostgresOperator(task_id='dag_stats', 
        sql="select dag_id,state,count(*) from dag_run WHERE dag_run.dag_id IN ( select dag_id from dag where scheduler_lock is null and is_paused='f' ) GROUP BY state, dag_id ORDER BY dag_id, state;",
        database="airflow"
    )

    influx = MyInfluxOperator(task_id='influx',
        xcom_task_id='dag_stats',
        measurement='airflow_dags',
        host='influxdb01.slac.stanford.edu',
    )


    dag_stats >> influx
