#!/bin/bash

function setup_user() {
    id -u ${U_NAME} 2> /dev/null 1>&2
    if [ $? -ne 0 ]; then
	make_user
    fi
}

function make_user() {
    # If EXTERNAL_UID is not set, we just use the standard system generated
    #  UID.
    # If we can get a good value, the UID and GID will both be that.
    # Reject implausibly small values.  Probably means we didn't get an
    #  ID and so we get the (small) serial assigned by KubeSpawner
    local nuid=""
    if [ "${EXTERNAL_UID}" -lt 100 ]; then
      EXTERNAL_UID=""
    fi
    if [ -n "${EXTERNAL_UID}" ]; then
      nuid=" ${EXTERNAL_UID}"
    fi
    add_groups
    local gentry=""
    local suppgrp=()
    local gid=""
    if [ -n "${EXTERNAL_GROUPS}" ]; then
      for gentry in $(echo ${EXTERNAL_GROUPS} | tr "," "\n"); do
        gname=$(echo ${gentry} | cut -d ':' -f 1)
        if [ -z "${gname}" ]; then
          continue
        fi
        local group_id=$(echo ${gentry} | cut -d ':' -f 1)
        if [ -z "${gid}" ]; then
            gid="${group_id}"
        fi
        supgrp+=("$gname")
      done
    fi
    echo adduser ${U_NAME} --disabled-password --home ${USER_HOMEDIR} -N --ingroup ${gid} --uid ${nuid} \
       --shell ${DEFAULT_SHELL} --gecos "jupyterhub account"
    adduser ${U_NAME} --disabled-password --home ${USER_HOMEDIR} -N --ingroup ${gid} --uid ${nuid} \
       --shell ${DEFAULT_SHELL} --gecos "jupyterhub account"
    for g in "${supgrp[@]}"; do
        echo adduser ${U_NAME} $g
        adduser ${U_NAME} $g
    done
    echo chown ${U_NAME}:$(id -gn ${U_NAME}) ${USER_HOMEDIR}
    chown ${U_NAME}:$(id -gn ${U_NAME}) ${USER_HOMEDIR}
}


function add_groups() {
    #add_group ${U_NAME} ${EXTERNAL_UID}
    local gentry=""
    local gname=""
    local gid=""
    if [ -n "${EXTERNAL_GROUPS}" ]; then
        for gentry in $(echo ${EXTERNAL_GROUPS} | tr "," "\n"); do
            gname=$(echo ${gentry} | cut -d ':' -f 1)
            gid=$(echo ${gentry} | cut -d ':' -f 2)
            add_group ${gname} ${gid}
        done
    fi
}

function add_group() {
    # If the group exists already, use that.
    # If it doesn't exist but the group id is in use, use a system-
    #  assigned gid.
    # Otherwise, use the group id to create the group.
    local gname=$1
    local gid=$2
    local exgrp=$(getent group ${gname})
    if [ -n "${exgrp}" ]; then
        return
    fi
    if [ -n "${gid}" ]; then
        local exgid=$(getent group ${gid})
        if [ -n "${exgid}" ]; then
            gid=""
        fi
    fi
    local gopt=""
    if [ -n "${gid}" ]; then
        gopt="-g ${gid}"
    fi
    #echo groupadd ${gopt} ${gname}
    groupadd ${gopt} ${gname}
}

function forget_extraneous_vars() {
    local purge="MEM_LIMIT CPU_LIMIT"
    unset ${purge}
    purge_docker_vars KUBERNETES HTTPS:443
}

function purge_docker_vars() {
    local n=$1
    local plist=$2
    local purge="${n}_PORT"
    local portmap=""
    local portname=""
    local portnum=""
    local i=""
    local k=""
    for i in "HOST" "PORT"; do
	purge="${purge} ${n}_SERVICE_${i}"
    done
    for portmap in $(echo ${plist} | tr "," "\n"); do
        portname=$(echo ${portmap} | cut -d ':' -f 1)
	purge="${purge} ${n}_SERVICE_PORT_${portname}"
        portnum=$(echo ${portmap} | cut -d ':' -f 2)
	for prot in "TCP" "UDP"; do
	    k="${n}_PORT_${portnum}_${prot}"
	    purge="${purge} ${k}"
	    for i in "ADDR" "PORT" "PROTO"; do
		purge="${purge} ${k}_${i}"
	    done
	done
    done
    unset ${purge}
}

## Begin mainline code. ##
U_NAME="${JUPYTERHUB_USER}"
USER_HOMEDIR="${AIRFLOW_HOME}"
DEFAULT_SHELL="/bin/bash"

sudo=""
if [ $(id -u) -eq 0 ]; then
    if [ -n "${U_NAME}" ]; then
	setup_user
	sudo="sudo -E -u ${U_NAME} "
    else
	echo 1>&2 "Warning: running as UID 0"
    fi
fi
forget_extraneous_vars

###
# prehook
###
chown ${U_NAME} ${AIRFLOW_HOME}/airflow.cfg
ls /usr/local/lib/python3.6/site-packages/airflow/www/templates/airflow/dags.html
sed -i 's/"bSort": false,/"order": [[ 3, "desc" ]],/g' /usr/local/lib/python3.6/site-packages/airflow/www/templates/airflow/dags.html
sed -i "s#            processor_poll_interval=1.0,#            processor_poll_interval=0.1,#" /usr/local/lib/python3.6/site-packages/airflow/jobs.py

###
# start airflow
###
exec ${sudo} /start-airflow.sh $@
