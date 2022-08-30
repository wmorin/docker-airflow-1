# VERSION 2.1.3
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM us.gcr.io/fathom-containers/debian-python3
LABEL maintainer="Puckel_"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Install security updates
RUN apt-get update -yqq \
    && apt-get install -yqq unattended-upgrades \
    && unattended-upgrade -v

# Airflow
ARG AIRFLOW_VERSION=2.1.3
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.6.txt"
ARG AIRFLOW_HOME=/usr/local/airflow

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8
ENV PYTHONPATH=:/usr/local/airflow/dags:/usr/local/airflow/config:/usr/src/config:/usr/src/diseaseTools
# To prevent Airflow from installing a GPL
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes

# See https://app.asana.com/0/911210587841072/1138807724977889/f
# for marshmallow-sqlalchemy=0.17.0
RUN set -ex \
    && buildDeps=' \
        python3-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        build-essential \
        python3-pip \
        python3-requests \
        default-mysql-client \
        default-mysql-server \
        default-libmysqlclient-dev \
        apt-utils \
        python-dev \
        libsasl2-dev \
        gcc \
        curl \
        rsync \
        libpq5 \
        netcat \
        locales \
        git \
        freetds-bin \
        krb5-user \
        ldap-utils \
        libffi6 \
        libsasl2-2 \
        libsasl2-modules \
        libssl1.1 \
        locales  \
        lsb-release \
        sasl2-bin \
        sqlite3 \
        unixodbc \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    # TODO: consider upgrading Airflow to v2, unpinning pip from 20.2.4 (see below)
    # https://app.asana.com/0/0/1199634198820229/f
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && pip install -U pip==21.2.4 \
    && pip install -U setuptools wheel \
    && pip install Cython \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install "apache-airflow[celery,postgres,apache.hive,jdbc,mysql,google,redis]==$AIRFLOW_VERSION" --constraint "${CONSTRAINT_URL}" \
    && pip install psycopg2 \
    && pip install pandas==0.23.4 \
    && pip install kubernetes==7.0.1 \
    && pip install google-cloud-logging==1.11.0 \
    && pip install retrying==1.3.3 \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY /entrypoint.sh /entrypoint.sh
ADD /webserver_config.py ${AIRFLOW_HOME}

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
