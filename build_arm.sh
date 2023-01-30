#!/usr/bin/env bash

set -euxo pipefail

repository=us.gcr.io/fathom-containers
tag=arm

IMAGES=(docker-airflow-two docker-airflow-two-test)
FOLDERS=(airflow_two airflow_two_test)

function build_and_push() {
    image=$1
    folder=$2

    pushd $image
    docker build . -t ${repository}/${image}:${tag} --build-arg FROM_TAG=${tag}
    # docker push ${repository}/${image}:${tag}
    popd
}

build_and_push docker-airflow-two airflow_two
build_and_push docker-airflow-two-test airflow_two_test