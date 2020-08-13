"""
# Suggestions
* Database: Anayltics
* Tables: conversations, messages

## Intermediary Storage (S3)
daily raw data conversation storage:
/agentiq-suggestion-data/{env}/daily_conversations_raw/{date}

daily transformed conversation data storage:
/agentiq-suggestion-data/{env}/daily_conversations_data/{date}

daily training conversation data storage:
/agentiq-suggestion-data/{env}/daily_training_data/{date}

daily suggestion models
/agentiq-ml-models/suggestions/daily/{env}/{year}/{month}/{day}

## Return
This dag generates custom model files for use by suggestion matcher in aiengine,
 it does the following:

* extracts today's conversations from analytics
* transforms it into format suitable for training
* collects all such training input files from s3
* collects ai_configs from ai-manager to also serve as inputs for training
* trains/ generates models which are used computation of suggestion ranks by ai-engine

"""

import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.airflow_helper import get_environments
from utils.download_nltk_models import download_nltk_data


default_args = {
    'owner': 'Akshay',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 15),
    'email': ['swe@agentiq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

dag = DAG('suggestions',
          default_args=default_args,
          catchup=False,
          # run every day at 1:30am PST
          schedule_interval='30 01 * * 1-7')
dag.doc_md = __doc__

# It is not recommanded to use Variable with global scope
# but not sure if there is another way to inject airflow variables
# into envionment variables.
env = os.environ.copy()
env.update(get_environments())

# # Dependent nltk data for topic.
# download_model = PythonOperator(
#     task_id='download_dependent_data',
#     python_callable=download_nltk_data,
#     dag=dag)
#
# extract_conversation = BashOperator(
#     task_id='extract_conversation',
#     bash_command='python3 -m tools.analysis.simple_stats --daily --upload_to_s3 \
#     --start_date="{{ execution_date.format("%Y-%m-%d") }} 00:00:00" \
#     --end_date="{{ execution_date.format("%Y-%m-%d") }} 23:59:59" \
#     --timezone="{{ var.value.TIMEZONE  }}"',
#     retries=1,
#     env=env,
#     dag=dag)
#
# transform_conversation = BashOperator(
#     task_id='transform_conversation',
#     bash_command="python3 -m tools.etl.extract_training_data \
#     --bucket_name=agentiq-suggestion-data \
#      --download_inputs_from_s3=True \
#      --action='upload'",
#     retries=1,
#     env=env,
#     dag=dag)
#
# collect_older_transformed_convos = BashOperator(
#     task_id='collect_older_transformed_convos',
#     bash_command="python3 -m tools.etl.extract_training_data \
#     --bucket_name=agentiq-suggestion-data  \
#     --action='clean_training_data'",
#     retries=1,
#     env=env,
#     dag=dag)
#
#
# collect_ai_configs = BashOperator(
#     task_id='collect_ai_configs',
#     bash_command='python3 -m tools.suggestions.pull_expressions_actions --upload_to_s3=True',
#     retries=1,
#     env=env,
#     dag=dag)


generate_suggestion_models = BashOperator(
    task_id='generate_suggestion_models',
    bash_command="python3 -m tools.suggestions.generate_model \
    --output=lm,vsm,dm  \
    --mode=generate \
    --download_inputs_from_s3=True",
    retries=1,
    env=env,
    dag=dag)


generate_suggestion_models
