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

