#!/bin/bash

# set -x

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

# roles
gen_template "rbac.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -

# create the secrets
gen_template "secrets.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -

# create the storage pv
gen_template "pv.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -
gen_template "pv-cryoem.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -

# create pvcs
gen_template "pvc.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -
gen_template "pvc-cryoem.yaml" | kubectl  -n ${namespace} ${ACTION} --record -f -

# create the hub
# this doesn't work for some reason
# gen_template "configmap.yaml" | kubectl -n ${namespace} ${ACTION} --record -f -
# TODO deal with a replace
if [ "${ACTION}" == "replace" ]; then
  kubectl -n ${namespace} delete configmap hub-config
fi
kubectl -n ${namespace} create configmap hub-config  --from-file=../config/jupyterhub_config.py  --from-file=../config/node-selectors.yaml --from-file=../config/jupyterhub_config.d --from-file=../config/images.d

gen_template "jupyterhub.yaml" | kubectl -n ${namespace} ${ACTION} --record -f -
