#!/usr/bin/env bash
# Setup Env variables for airflow

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
/bin/bash $DIR/import.sh

if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
  AIRFLOW__CELERY__BROKER_URL="redis://$REDIS_PREFIX$REDIS_HOST:$REDIS_PORT/1"
  wait_for_port "Redis" "$REDIS_HOST" "$REDIS_PORT"
fi

airflow initdb
if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
  # With the "Local" and "Sequential" executors it should all run in one container.
  airflow scheduler &
  python script/env_export_to_json.py > exported_variables.json
  airflow variables --import exported_variables.json
fi
#exec airflow webserver
exec airflow list_dags
