FROM gcr.io/fathom-containers/docker-airflow

COPY requirements.txt /usr/local/airflow

RUN apt-get install -y git

RUN pip install --no-cache-dir -r requirements.txt
