FROM 335471400721.dkr.ecr.us-east-1.amazonaws.com/airflow:18eca6c
USER airflow
WORKDIR ${AIRFLOW_HOME}
#ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
