# AIQ-airflow
This branch is forked from [here](https://github.com/puckel/docker-airflow) that shows multiple usage of docker.
The Dockerfile is slightly modified to use our python base image and resolved dependecies accordingly.
 
## References
Here are some references how to write dags and learn best practices.
 - [Airflow Doc](https://airflow.apache.org/)
 - [ETL Best Practice](https://gtoonstra.github.io/etl-with-airflow/)


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

## Quick Operator Guide
Dags in airflow are a group of tasks defined with `Operator`s. There are numerous operators supported by Airflow and here are some useful operator and indicates when to use.
- [BashOperator](https://airflow.apache.org/_api/airflow/operators/bash_operator/index.html): Running bash script or any linux command. Also, docker command is available through here.
- [BaseSensorOperator](https://airflow.apache.org/_api/index.html#basesensoroperator): The typical usage of SensorOperator is to wait and sense something changed.
- [PostgresOperator](https://airflow.apache.org/_api/airflow/operators/postgres_operator/index.html): To run a raw queries on postgres database, this is the operator to use
- [EmailOperator](https://airflow.apache.org/_api/airflow/operators/email_operator/index.html): To send email
- [PythonOperator](https://airflow.apache.org/_api/airflow/operators/python_operator/index.html): Running a python function.
- [S3FileTransformOperator](https://airflow.apache.org/_api/airflow/operators/s3_file_transform_operator/index.html): Using aws s3.
- [SimpleHttpOperator](https://airflow.apache.org/_api/airflow/operators/http_operator/index.html): Running Http request
