# VERSION 1.10.15
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM 036978135238.dkr.ecr.us-east-1.amazonaws.com/agentiq/app-python:3.8-buster-v1

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# # Airflow
ARG AIRFLOW_VERSION=2.2.5
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_VERSION=3.8
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}


RUN apt-get --allow-releaseinfo-change update -yqq \
    && apt-get upgrade -yqq

RUN apt-get install -y apt-utils
RUN apt-get install -yqq --no-install-recommends \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
        freetds-bin \
        build-essential \
        curl \
        rsync \
        netcat \
        locales \
        jq \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi

RUN pip install -U setuptools wheel \
  && pip install pytz \
  && pip install pyOpenSSL==22.0.0 \
  && pip install ndg-httpsclient \
  && pip install flake8 \
  && pip install pytest \
  && pip install pyasn1

# Added constraint file from airflow documentation: https://airflow.apache.org/docs/apache-airflow/2.2.5/installation/installing-from-pypi.html#installation-tools
RUN pip install apache-airflow[crypto,celery,postgres,hive,jdbc,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"\
    && pip install 'redis==3.2'

# Fix added for airflow failure
RUN pip uninstall -y SQLAlchemy \
  && pip install SQLAlchemy==1.3.18 \
  && pip install wtforms==2.3.3

RUN apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base


# aws dependency
RUN apt-get install unzip && cd /tmp && \
    curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip" && \
    unzip awscli-bundle.zip && \
    ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws && \
    rm awscli-bundle.zip && rm -rf awscli-bundle

# Presently broken
RUN curl -fsSL https://get.docker.com | sh

# Let's start with some basic stuff.
RUN apt-get update && \
    apt-get install -qqy \
    apt-transport-https \
    ca-certificates \
    curl \
    lxc \
    iptables \
    gnupg \
    lsb-release

# # We should pin docker-ce* (e.g. apt-get install docker-ce=$VERSION docker-ce-cli=$VERSION containerd.io)
# RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
#     gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
#     echo \
#     "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
#     $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
#     apt-get install docker-ce docker-ce-cli containerd.io


# Further dependencies should go the below
RUN apt-get install -qqy \
    expect

# Define additional metadata for our image.
VOLUME /var/lib/docker


COPY script/entrypoint.sh /entrypoint.sh
COPY script/startup.sh /startup.sh
COPY ./script ${AIRFLOW_USER_HOME}/script
COPY airflow_config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
COPY ./dags ${AIRFLOW_USER_HOME}/dags
COPY Makefile ${AIRFLOW_USER_HOME}/Makefile
COPY ./tests ${AIRFLOW_USER_HOME}/tests
COPY variables.json ${AIRFLOW_USER_HOME}/variables.json
COPY ./python-tools ${AIRFLOW_USER_HOME}/python-tools
COPY ./aiq-dynamo-python ${AIRFLOW_USER_HOME}/aiq-dynamo-python

WORKDIR ${AIRFLOW_USER_HOME}
RUN pip install -r python-tools/requirements.txt
RUN pip install -r aiq-dynamo-python/requirements.txt

EXPOSE 8080 5555 8793

ENV PATH "$PATH:/usr/local/airflow/dags/bin"
ENV PYTHONPATH "$PYTHONPATH:$AIRFLOW_HOME/python-tools:$AIRFLOW_HOME/aiq-dynamo-python"

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/startup.sh"]
