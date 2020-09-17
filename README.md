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
- set up PYTHONPATH locally, airflow uses this variable to search for
  python modules and without it, DAGs cannot access submodules. Set it up
  as follows
  ```
    export AIQ_AIRFLOW_HOME={path to you airflow repo}
    if
        PYTHONPATH is empty , do
        export PYTHONPATH="$AIQ_AIRFLOW_HOME/python-tools:$AIQ_AIRFLOW_HOME/aiq-dynamo-python"
    otherwise, do:
        export PYTHONPATH="$PYTHONPATH:$AIQ_AIRFLOW_HOME/python-tools:$AIQ_AIRFLOW_HOME/aiq-dynamo-python"
  
  ```
-   install requirements from python tools and aiq-dynamo sub modules
    by cding into them and running
    ```
    pip3 install -r requirements.txt
    ```
-  If you don't have submodules locally,
    run
    ```
    make checkout
    make update
    ```
- Generate Variables:

    After setting up airflow, each task uses Airflow Variables to get environment specific values. To import airflow variables, you may use a script to generate from your environment file as the below
    ```
    source ~/.agentiq/{env}
    python script/env_export_to_json.py    // To see the output in stdout
    or
    python script/env_export_to_json.py > vars.json   // To save the output in a file
    
    ```
- Put all Variables from all DAGs in variables.json and import them using
    ```
    airflow variables -i vars.json
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
## Seeding airflow instance
- Add Database connections to Admin > Connections
- DAGS need to be turned on upon release, by default, they are off

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

Once the file is available, go to Admin > Variables > Import in the airflow UI and upload the file(vars.json)

## Persist Variables
Add FERNET_KEY for encryption key to persist variables.
https://airflow.readthedocs.io/en/stable/howto/secure-connections.html

## Adding environment variables to Airflow Variables
In the bootstrap of airflow, it looks at `variables.json` and push it into airflow variables. To add an new airflow variable from environment variables, specify the name of the variable as a key such as 'BASE_API_TOKEN' and add value as an empty string. It would populate the right value when it is deployed.


## Quick Operator Guide
Dags in airflow are a group of tasks defined with `Operator`s. There are numerous operators supported by Airflow and here are some useful operator and indicates when to use.
- [BashOperator](https://airflow.apache.org/_api/airflow/operators/bash_operator/index.html): Running bash script or any linux command. Also, docker command is available through here.
- [BaseSensorOperator](https://airflow.apache.org/_api/index.html#basesensoroperator): The typical usage of SensorOperator is to wait and sense something changed.
- [PostgresOperator](https://airflow.apache.org/_api/airflow/operators/postgres_operator/index.html): To run a raw queries on postgres database, this is the operator to use
- [EmailOperator](https://airflow.apache.org/_api/airflow/operators/email_operator/index.html): To send email
- [PythonOperator](https://airflow.apache.org/_api/airflow/operators/python_operator/index.html): Running a python function.
- [S3FileTransformOperator](https://airflow.apache.org/_api/airflow/operators/s3_file_transform_operator/index.html): Using aws s3.
- [SimpleHttpOperator](https://airflow.apache.org/_api/airflow/operators/http_operator/index.html): Running Http request
 
