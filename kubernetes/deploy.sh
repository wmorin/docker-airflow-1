#!/bin/bash

set -x

CONFIG_FILE="ENVIRONMENT"
if [ -n "${1}" ]; then
    CONFIG_FILE="${1}"
fi

# import key value pairs
source ./${CONFIG_FILE}

# action
ACTION="${2}"
if [ "${ACTION}" = "--update" ] ||  [ "${ACTION}" = "--upgrade" ]; then
  ACTION="replace"
else
  ACTION="apply"
fi 

# stolen from https://starkandwayne.com/blog/bashing-your-yaml/
function gen_template() {
    rm -f final.yaml temp.yaml  
    ( echo "cat <<EOF >final.yaml";
      cat $1;
      echo "EOF";
    ) >temp.yaml
    . temp.yaml
    cat final.yaml
    rm -f final.yaml temp.yaml  
}

# create the namespace for this project
kubectl create namespace ${namespace}

# create the secrets
gen_template "secrets.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -

# create the storage pv
gen_template "database-storage.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -
gen_template "messagebus-storage.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -
gen_template "airflow-storage.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -
gen_template "cryoem-storage.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -
sleep 2

# create the hub
# this doesn't work for some reason
# gen_template "configmap.yaml" | kubectl -n ${namespace} ${ACTION} --record -f -
# TODO deal with a replace
#if [ "${ACTION}" == "replace" ]; then
#  kubectl -n ${namespace} delete configmap hub-config
#fi
#kubectl -n ${namespace} create configmap hub-config  --from-file=../config/jupyterhub_config.py  --from-file=../config/node-selectors.yaml --from-file=../config/jupyterhub_config.d --from-file=../config/images.d

# create the db
gen_template "database.yaml" | kubectl -n ${namespace} ${ACTION} --record -f -

# create the message bus
gen_template "messagebus.yaml" | kubectl -n ${namespace} ${ACTION} --record -f -

# create the webserver
gen_template "webserver.yaml" | kubectl -n ${namespace} ${ACTION} --record -f -

# create the scheduler
gen_template "scheduler.yaml" | kubectl -n ${namespace} ${ACTION} --record -f -

# create the webserver
gen_template "worker.yaml" | kubectl -n ${namespace} ${ACTION} --record -f -
