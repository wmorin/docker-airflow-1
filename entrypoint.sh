#!/usr/bin/env bash

AIRFLOW_HOME=${AIRFLOW_HOME:-/usr/local/airflow}

CMD="airflow"
TRY_LOOP="5"

REDIS_SERVICE_HOST=${REDIS_SERVICE_HOST:-"redis"}
REDIS_SERVICE_PORT=${REDIS_SERVICE_PORT:-"6379"}
REDIS_PASSWORD=$( echo $REDIS_CONFIG | grep requirepass | sed 's|^requirepass \(.*\)\s*|\1|' )
if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=:${REDIS_PASSWORD}@
else
    REDIS_PREFIX=""
fi

POSTGRES_SERVICE_HOST=${POSTGRES_SERVICE_HOST:-"postgres"}
POSTGRES_SERVICE_PORT=${POSTGRES_SERVICE_PORT:-"5432"}
POSTGRES_PASSWORD=$(echo ${POSTGRES_PASSWORD%\\n:-airflow} | tr -d '\n' )
POSTGRES_DB=${POSTGRES_DB:-"airflow"}

DAGS_DIR=${DAGS_DIR:-"/usr/local/airflow/dags"}
DAGS_PLUGIN_DIR=${DAGS_PLUGIN_DIR:-"/usr/local/airflow/plugins"}
DAGS_LOG_DIR=${DAGS_LOG_DIR:-"/usr/local/airflow/logs"}

FERNET_KEY=$(echo ${FERNET_KEY%\\n:-$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")} | tr -d '\n') 

AIRFLOW_EXECUTOR=${EXECUTOR:-Celery}
AIRFLOW_DEFAULT_QUEUE=${AIRFLOW_DEFAULT_QUEUE:-default}
AIRFLOW_PARALLELISM=${AIRFLOW_PARALLELISM:-256}
AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG=${AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG:-64}
AIRFLOW_DAG_CONCURRENCY=${AIRFLOW_DAG_CONCURRENCY:-256}
AIRFLOW_MAX_THREADS=${AIRFLOW_MAX_THREADS:-256}

#: ${STATSD_HOST:-"localhost"}
#: ${STATSD_PORT:-"8125"}
#: ${STATSD_PREFIX:-"airflow"}

# Update airflow config
echo "Modifying airflow.cfg..."
sed -i "s|result_backend = .*\$|result_backend = db+postgresql://$POSTGRES_USER:${POSTGRES_PASSWORD%\\n}@$POSTGRES_SERVICE_HOST:$POSTGRES_SERVICE_PORT/$POSTGRES_DB|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|sql_alchemy_conn = .*\$|sql_alchemy_conn = postgresql+psycopg2://$POSTGRES_USER:${POSTGRES_PASSWORD%\\n}@$POSTGRES_SERVICE_HOST:$POSTGRES_SERVICE_PORT/$POSTGRES_DB|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|broker_url = .*\$|broker_url = redis://${REDIS_PREFIX}${REDIS_SERVICE_HOST}:${REDIS_SERVICE_PORT}/1|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^default_queue = .*\$|default_queue = ${AIRFLOW_DEFAULT_QUEUE}|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^parallelism = .*\$|parallelism = ${AIRFLOW_PARALLELISM}|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^non_pooled_task_slot_count = .*\$|non_pooled_task_slot_count = ${AIRFLOW_PARALLELISM}|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^dag_concurrency = .*\$|dag_concurrency = ${AIRFLOW_DAG_CONCURRENCY}|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^max_threads = .*\$|max_threads = ${AIRFLOW_MAX_THREADS}|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^max_active_runs_per_dag = .*\$|max_active_runs_per_dag = ${AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG}|" $AIRFLOW_HOME/airflow.cfg
# increase scheduling speed
# sed -i "s#            processor_poll_interval=1.0,#            processor_poll_interval=$PROCESSOR_POLL_INTERVAL,#" /usr/local/lib/python3.6/site-packages/airflow/jobs.py
#sed -i "s|statsd_host = .*\$|statsd_host = $STATSD_HOST|" $AIRFLOW_HOME/airflow.cfg
#sed -i "s|statsd_port = .*\$|statsd_port = $STATSD_PORT|" $AIRFLOW_HOME/airflow.cfg
#sed -i "s|statsd_prefix = .*\$|statsd_prefix = $STATSD_PREFIX|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^fernet_key = .*\$|fernet_key = ${FERNET_KEY%\\n}|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^dags_folder = .*\$|dags_folder = ${DAGS_DIR}|g" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^base_log_folder = .*\$|base_log_folder = ${DAGS_LOG_DIR}|g" $AIRFLOW_HOME/airflow.cfg
sed -i "s|^plugins_folder = .*\$|plugins_folder = ${DAGS_PLUGIN_DIR}|g" $AIRFLOW_HOME/airflow.cfg

echo '=========='
cat $AIRFLOW_HOME/airflow.cfg | grep -vE '(^\#|^$)'
echo '=========='

echo "Starting Airflow in ${AIRFLOW_MODE} mode...."

# Wait for Postresql
if [[ "${AIRFLOW_MODE}" == "webserver" || "${AIRFLOW_MODE}" == "worker" || "${AIRFLOW_MODE}" == "scheduler" ]] ; then
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
if [ "$AIRFLOW_EXECUTOR" == "Celery" ]
then
  # Wait for Redis
  if [[ "${AIRFLOW_MODE}" == "webserver" || "${AIRFLOW_MODE}" = "worker" || "${AIRFLOW_MODE}" = "scheduler" || "${AIRFLOW_MODE}" = "flower" ]] ; then
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

  if [ "${AIRFLOW_MODE}" == "webserver" ]; then
    # init db
    echo exec $CMD initdb
    $CMD initdb
  #elif [ "${AIRFLOW_MODE}" == "scheduler" ]; then
  #  sleep 5
  fi
  echo Running... $CMD ${AIRFLOW_MODE}
  exec $CMD ${AIRFLOW_MODE}

# spit out version and exit
else
  echo "Only Celery Excutor supported... exiting."
  exec $CMD version
  exit
fi
