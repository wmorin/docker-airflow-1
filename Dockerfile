# VERSION 1.8.1-1
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t yee379/docker-airflow .
# SOURCE: https://github.com/slaclab/cryoem-airflow

FROM python:3.6-slim
MAINTAINER yee379

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# Airflow
ARG AIRFLOW_VERSION=1.10.1
ARG AIRFLOW_HOME=/usr/local/airflow

#RUN set -ex \
#    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

ARG fetchDeps='ca-certificates wget curl'
ARG buildDeps='python3-dev libkrb5-dev libsasl2-dev libssl-dev libffi-dev build-essential libblas-dev liblapack-dev libpq-dev git'

RUN set -ex \
    && apt-get update -yqq \    
    && apt-get install -yqq --no-install-recommends \
        $fetchDeps \
        $buildDeps \
        python3-pip \
        python3-requests \
        apt-utils \
        locales \
        netcat \
        sudo \
        rsync \
        parallel \
        openssh-client \
        libsys-hostname-long-perl \
        imagemagick \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 

# install python related stuff
RUN set -ex \
    && python -m pip install -U pip==9.0.1 setuptools wheel \
    && pip install Cython \
        pytz \
        pyOpenSSL \
        ndg-httpsclient \
        pyasn1 \
        statsd \
        influxdb \
        slackclient \
        paramiko \
        sshtunnel
    
RUN set -ex \
    && SLUGIFY_USES_TEXT_UNIDECODE=yes \
      pip install \
        apache-airflow[crypto,celery,postgres,hive,jdbc]==$AIRFLOW_VERSION \
        'redis==2.10.6' \
        'celery[redis]>=4.1.1,<4.2.0'

# clean up everything
RUN set -ex \
    && apt-get purge --auto-remove -yqq $buildDeps $fetchDeps \
    && apt-get clean \
    && rm -rf \
        /root/.cache \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY scripts/entrypoint.sh /entrypoint.sh
COPY scripts/start-airflow.sh /start-airflow.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

ENV AIRFLOW_HOME $AIRFLOW_HOME

WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
