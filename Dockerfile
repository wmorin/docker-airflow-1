FROM  apache/airflow:2.1.2-python3.8

#LABEL version="1.0.0"

RUN pip install --user pytest && pip install --user awscli && pip install --user flask-bcrypt && pip install awswrangler==2.11.0 && pip install pip install solana==0.15.1

COPY dags/ ${AIRFLOW_HOME}/dags
COPY unittests.cfg ${AIRFLOW_HOME}/unittests.cfg
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY unittests/ ${AIRFLOW_HOME}/unittests
COPY integrationtests ${AIRFLOW_HOME}/integrationtests
