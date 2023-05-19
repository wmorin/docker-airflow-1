# docker-airflow

This repository contains **Dockerfiles** for building Docker images containing
[Airflow 2](https://github.com/apache/incubator-airflow) which are published to Google
Container Registry in the `fathom-containers` project.

## Information

-   Based on Python (3.6-slim) official Image [python:3.6-slim](https://hub.docker.com/_/python/)
    and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and
    [Redis](https://hub.docker.com/_/redis/) as the queue.

-   Install [Docker](https://www.docker.com/)

-   Uses the Airflow release from
    [PYPI (Python Package Index)](https://pypi.python.org/pypi/apache-airflow)

## Configuring Airflow

It's possible to set any configuration value for Airflow from environment variables,
which are used over values from the airflow.cfg.

The general rule is the environment variable should be named `AIRFLOW__<section>__<key>`
, for example `AIRFLOW__CORE__SQL_ALCHEMY_CONN` sets the `sql_alchemy_conn`
config option in the `[core]` section.

Check out the 
[Airflow documentation](http://airflow.readthedocs.io/en/latest/howto/set-config.html#setting-configuration-options)
for more details

You can also define connections via environment variables by prefixing them with
`AIRFLOW_CONN_` - for example
`AIRFLOW_CONN_POSTGRES_MASTER=postgres://user:password@localhost:5432/master`
for a connection called "postgres_master". The value is parsed as a URI. This will 
work for hooks etc, but won't show up in the "Ad-hoc Query" section unless an (empty)
connection is also created in the DB.
