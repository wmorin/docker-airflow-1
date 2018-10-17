FROM gcr.io/fathom-containers/docker-airflow

COPY requirements.txt /usr/local/airflow

RUN apt-get install -yqq --no-install-recommends git

RUN pip install --no-cache-dir -r requirements.txt
