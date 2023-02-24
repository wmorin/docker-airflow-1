FROM docker-airflow-bridge-fix
USER root
COPY requirements.txt /usr/local/airflow
RUN pip install --no-cache-dir -r requirements.txt
USER airflow