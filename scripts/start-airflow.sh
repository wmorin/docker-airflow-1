#!/usr/bin/env bash

: ${AIRFLOW_HOME:="/usr/local/airflow"}
CMD="airflow"
TRY_LOOP="5"

: ${REDIS_SERVICE_HOST:="redis"}
: ${REDIS_SERVICE_PORT:="6379"}
: ${REDIS_PASSWORD:=""}

: ${POSTGRES_SERVICE_HOST:="postgres"}
: ${POSTGRES_SERVICE_PORT:="5432"}
: ${POSTGRES_USER:="airflow"}
: ${POSTGRES_PASSWORD:="airflow"}
: ${POSTGRES_DB:="airflow"}

: ${DAGS_DIR:="/usr/local/airflow/dags"}
: ${DAGS_PLUGIN_DIR:="/usr/local/airflow/plugins"}
: ${DAGS_LOG_DIR:="/usr/local/airflow/logs"}{

: ${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}

: ${EXECUTOR:=Celery}

: ${STATSD_HOST:="localhost"}
: ${STATSD_PORT:="8125"}
: ${STATSD_PREFIX:="airflow"}

# Load DAGs exemples (default: Yes)
if [ "$LOAD_EX" = "n" ]; then
    sed -i "s/load_examples = True/load_examples = False/" "$AIRFLOW_HOME"/airflow.cfg
fi

# Update airflow config - Fernet key
sed -i "s|\$FERNET_KEY|$FERNET_KEY|" "$AIRFLOW_HOME"/airflow.cfg
sed -i "s#^dags_folder = .*\$#dags_folder = ${DAGS_DIR}#g" "$AIRFLOW_HOME"/airflow.cfg
sed -i "s#^base_log_folder = .*\$#base_log_folder = ${DAGS_LOG_DIR}#g" "$AIRFLOW_HOME"/airflow.cfg
sed -i "s#^plugins_folder = .*\$#plugins_folder = ${DAGS_PLUGIN_DIR}#g" "$AIRFLOW_HOME"/airflow.cfg

if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=:${REDIS_PASSWORD}@
else
    REDIS_PREFIX=
fi

AIRFLOW_MODE="${AIRFLOW_MODE}"
if [ -z "${AIRFLOW_MODE}" ]; then
  AIRFLOW_MODE="${AIRFLOW_MODE}"
fi

echo "Starting ${AIRFLOW_MODE}...."

# Wait for Postresql
if [ "${AIRFLOW_MODE}" = "webserver" ] || [ "${AIRFLOW_MODE}" = "worker" ] || [ "${AIRFLOW_MODE}" = "scheduler" ] ; then
  i=0
  echo "Checking db connection to $POSTGRES_SERVICE_HOST:$POSTGRES_SERVICE_PORT..."
  while ! nc -z $POSTGRES_SERVICE_HOST $POSTGRES_SERVICE_PORT >/dev/null 2>&1 < /dev/null; do
    i=$((i+1))
    if [ "${AIRFLOW_MODE}" = "webserver" ]; then
      echo "$(date) - waiting for ${POSTGRES_SERVICE_HOST}:${POSTGRES_SERVICE_PORT}... $i/$TRY_LOOP"
      if [ $i -ge $TRY_LOOP ]; then
        echo "$(date) - ${POSTGRES_SERVICE_HOST}:${POSTGRES_SERVICE_PORT} still not reachable, giving up"
        exit 1
      fi
    fi
    sleep 10
  done
fi

# Update configuration depending the type of Executor
if [ "$EXECUTOR" = "Celery" ]
then
  # Wait for Redis
  if [ "${AIRFLOW_MODE}" = "webserver" ] || [ "${AIRFLOW_MODE}" = "worker" ] || [ "${AIRFLOW_MODE}" = "scheduler" ] || [ "${AIRFLOW_MODE}" = "flower" ] ; then
    j=0
    echo "Checking redis connection to $REDIS_SERVICE_HOST:$REDIS_SERVICE_PORT..."
    while ! nc -z $REDIS_SERVICE_HOST $REDIS_SERVICE_PORT >/dev/null 2>&1 < /dev/null; do
      j=$((j+1))
      if [ $j -ge $TRY_LOOP ]; then
        echo "$(date) - $REDIS_SERVICE_HOST still not reachable, giving up"
        exit 1
      fi
      echo "$(date) - waiting for Redis... $j/$TRY_LOOP"
      sleep 5
    done
  fi
  sed -i "s#celery_result_backend = db+postgresql://airflow:airflow@postgres/airflow#celery_result_backend = db+postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_SERVICE_HOST:$POSTGRES_SERVICE_PORT/$POSTGRES_DB#" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_SERVICE_HOST:$POSTGRES_SERVICE_PORT/$POSTGRES_DB#" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#broker_url = redis://redis:6379/1#broker_url = redis://$REDIS_PREFIX$REDIS_SERVICE_HOST:$REDIS_SERVICE_PORT/1#" "$AIRFLOW_HOME"/airflow.cfg
  # increase scheduling speed
  # sed -i "s#            processor_poll_interval=1.0,#            processor_poll_interval=$PROCESSOR_POLL_INTERVAL,#" /usr/local/lib/python3.6/site-packages/airflow/jobs.py
  sed -i "s#statsd_host = .*\$#statsd_host = $STATSD_HOST#" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#statsd_port = .*\$#statsd_port = $STATSD_PORT#" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#statsd_prefix = .*\$#statsd_prefix = $STATSD_PREFIX#" "$AIRFLOW_HOME"/airflow.cfg

  echo '=========='
  cat $AIRFLOW_HOME/airflow.cfg | grep -vE '^\#'
  echo '=========='

  if [ "${AIRFLOW_MODE}" = "webserver" ]; then
    # init db
    echo exec $CMD initdb
    $CMD initdb
  else
    sleep 10
  fi
  echo exec $CMD "${AIRFLOW_MODE}"
  exec $CMD "${AIRFLOW_MODE}"

# spit out version and exit
else
  exec $CMD version
  exit
fi
