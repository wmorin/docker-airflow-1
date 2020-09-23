export AIRFLOW_HOME=.
export AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW_HOME/dags
export AIRFLOW__CORE__BASE_LOG_FOLDER=$AIRFLOW_HOME/logs
export PYTHONPATH=$PYTHONPATH:$AIRFLOW_HOME/python-tools:$AIRFLOW_HOME/aiq-dynamo-python

source script/import.sh
