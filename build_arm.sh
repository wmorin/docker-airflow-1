#!/usr/bin/env bash

set -euxo pipefail

repository=us.gcr.io/fathom-containers
tag=arm
IMAGES=(airflow_two airflow_two_test)

for image in ${IMAGES[*]}
do
    pushd $image
    docker build . -t ${repository}/${image}:${tag} --build-arg FROM_TAG=${tag}
    docker push ${repository}/${image}:${tag}
    popd
done