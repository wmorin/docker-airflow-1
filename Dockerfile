FROM apache/airflow:2.2.2-python3.8

MAINTAINER jbarry <john.barry@leaftrade.com>

COPY requirements.txt /requirements.txt

RUN  pip install pytz \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install 'apache-airflow[amazon,google,celery,password,postgres]==2.2.2'\
    && pip install --requirement /requirements.txt 

COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN curl https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem > ${AIRFLOW_HOME}/awssslcert.pem
#RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
#ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
