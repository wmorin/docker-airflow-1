#!/usr/bin/env bash
# Setup Env variables for airflow


TRY_LOOP="20"

# TODO: If we switch to Celery, this needs to be configured
: "${REDIS_HOST:="redis"}"
: "${REDIS_PORT:="6379"}"
: "${REDIS_PASSWORD:=""}"

: "${POSTGRES_HOST:=${AIQ_AIRFLOW_DB_HOST}}"
: "${POSTGRES_PORT:=${AIQ_AIRFLOW_DB_PORT}}"
: "${POSTGRES_USER:=${AIQ_AIRFLOW_DB_USERNAME}}"
: "${POSTGRES_PASSWORD:=${AIQ_AIRFLOW_DB_PASSWORD}}"
: "${POSTGRES_DB:=${AIQ_AIRFLOW_DB_NAME}}"

echo "Connecting to $POSTGRES_HOST for $POSTGRES_DB using $POSTGRES_USER"


# Defaults and back-compat
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"

echo "Setting SMTP Host to $AIRFLOW__SMTP__SMTP_HOST"
echo "Setting Core Executor tp $AIRFLOW__CORE__EXECUTOR"

export \
  AIRFLOW_HOME \
  AIRFLOW__CELERY__BROKER_URL \
  AIRFLOW__CELERY__RESULT_BACKEND \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__SMTP__SMTP_HOST \
  AIRFLOW__SMTP__SMTP_USER \
  AIRFLOW__SMTP__SMTP_PASSWORD \
  AIRFLOW__SMTP__SMTP_PORT \
  AIRFLOW__SMTP__SMTP_STARTTLS \
  AIRFLOW__SMTP__SMTP_SSL \
  AIRFLOW__SMTP__SMTP_MAIL_FROM \

# Load DAGs exemples (default: Yes)
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]
then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi

# Install custom python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(command -v pip) install --user -r /requirements.txt
fi

if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=:${REDIS_PASSWORD}@
else
    REDIS_PREFIX=
fi

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"

