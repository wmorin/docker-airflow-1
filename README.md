# AIQ-airflow
This branch is forked from [here](https://github.com/puckel/docker-airflow) that shows multiple usage of docker.
The Dockerfile is slightly modified to use our python base image and resolved dependecies accordingly.
 
## References
Here are some references how to write dags and learn best practices.
 - [Airflow Doc](https://airflow.apache.org/)
 - [ETL Best Practice](https://gtoonstra.github.io/etl-with-airflow/)

## Running locally without docker
- Make sure ~/airflow/airflow.cfg point to the the dags folder, it can be done using
    ```
    export PATH_TO_AIRFLOW_DAGS=$(pwd)/dags
    sed -i ".original" "s|\(dags_folder *= *\).*|\1$PATH_TO_AIRFLOW_DAGS|" ~/airflow/airflow.cfg

    ```
- Put all Variables from all DAGs in variables.json and import them using
    ```
    airflow variables -i variables.json
    ```
- initialize airflow DB  ```airflow initdb```
- run airflow server ```airflow webserver -p 8080```
- run airflow scheduler ```airflow scheduler```
  If the scheduler gets stuck resetting old DAG runs, old DAGs could be deleted
  at localhost:8080 > Browse > DAG runs
- trigger your DAG   localhost:8080 > {{name of your DAG}} > trigger
If tasks do not run and Task Instance Details reveal that the DAG is paused,
run
```
airflow unpause {{name of your DAG}}
```
## Running using docker
The source can run with docker easily if you have database setup. For example, if your database is running with the below params
```
AIQ_AIRFLOW_DB_HOST=192.168.1.228
AIQ_AIRFLOW_DB_PORT=5432
AIQ_AIRFLOW_DB_USER=postgres
AIQ_AIRFLOW_DB_PASSWORD=password
AIQ_AIRFLOW_DB_NAME=test_airflow
ENVIRONMENT=s1
```
Then, use the below command to run the server locally.

```
docker run -p 8080:8080 -e "AIQ_AIRFLOW_DB_HOST=192.168.1.228" -e "AIQ_AIRFLOW_DB_PORT=5432" -e "AIQ_AIRFLOW_DB_USER=postgres" -e "AIQ_AIRFLOW_DB_PASSWORD=password" -e "AIQ_AIRFLOW_DB_NAME=test_airflow"  -e "ENVIRONMENT=s1"
```

## Generate Variables
After setting up airflow, each task uses Airflow Variables to get environment specific values. To import airflow variables, you may use a script to generate from your environment file as the below
```
source ~/.agentiq/{env}
python tools/env_export_to_json.py    // To see the output in stdout
or
python tools/env_export_to_json.py > vars.json   // To save the output in a file

```
Once the file is available, go to Admin > Variables > Import in the airflow UI and upload the file(vars.json)

## Persist Variables
Add FERNET_KEY for encryption key to persist variables.
https://airflow.readthedocs.io/en/stable/howto/secure-connections.html


## Quick Operator Guide
Dags in airflow are a group of tasks defined with `Operator`s. There are numerous operators supported by Airflow and here are some useful operator and indicates when to use.
- [BashOperator](https://airflow.apache.org/_api/airflow/operators/bash_operator/index.html): Running bash script or any linux command. Also, docker command is available through here.
- [BaseSensorOperator](https://airflow.apache.org/_api/index.html#basesensoroperator): The typical usage of SensorOperator is to wait and sense something changed.
- [PostgresOperator](https://airflow.apache.org/_api/airflow/operators/postgres_operator/index.html): To run a raw queries on postgres database, this is the operator to use
- [EmailOperator](https://airflow.apache.org/_api/airflow/operators/email_operator/index.html): To send email
- [PythonOperator](https://airflow.apache.org/_api/airflow/operators/python_operator/index.html): Running a python function.
- [S3FileTransformOperator](https://airflow.apache.org/_api/airflow/operators/s3_file_transform_operator/index.html): Using aws s3.
- [SimpleHttpOperator](https://airflow.apache.org/_api/airflow/operators/http_operator/index.html): Running Http request
 
