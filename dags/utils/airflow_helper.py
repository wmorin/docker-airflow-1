from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


def get_environments():
    envs = {'ENVIRONMENT': Variable.get('ENVIRONMENT'),
            'BASE_API_DB_PORT':  Variable.get('BASE_API_DB_PORT'),
            'BASE_API_DB_HOST': Variable.get('BASE_API_DB_HOST'),
            'BASE_API_DB_NAME': Variable.get('BASE_API_DB_NAME'),
            'BASE_API_DB_USERNAME': Variable.get('BASE_API_DB_USERNAME'),
            'BASE_API_DB_PASSWORD': Variable.get('BASE_API_DB_PASSWORD'),
            'BASE_API_TOKEN': Variable.get('BASE_API_TOKEN'),
            'ANALYTICS_DB_PORT': Variable.get('ANALYTICS_DB_PORT'),
            'ANALYTICS_DB_HOST': Variable.get('ANALYTICS_DB_HOST'),
            'ANALYTICS_DB_NAME': Variable.get('ANALYTICS_DB_NAME'),
            'ANALYTICS_DB_USERNAME': Variable.get('ANALYTICS_DB_USERNAME'),
            'ANALYTICS_DB_PASSWORD': Variable.get('ANALYTICS_DB_PASSWORD'),
            'AI_MANAGER_DB_HOST': Variable.get('AI_MANAGER_DB_HOST'),
            'AI_MANAGER_DB_PORT': Variable.get('AI_MANAGER_DB_PORT'),
            'AI_MANAGER_DB_USERNAME': Variable.get('AI_MANAGER_DB_USERNAME'),
            'AI_MANAGER_DB_PASSWORD': Variable.get('AI_MANAGER_DB_PASSWORD'),
            'AI_MANAGER_DB_NAME': Variable.get('AI_MANAGER_DB_NAME'),
            'STATS_DB_PORT': Variable.get('STATS_DB_PORT'),
            'STATS_DB_HOST': Variable.get('STATS_DB_HOST'),
            'STATS_DB_NAME': Variable.get('STATS_DB_NAME'),
            'STATS_DB_USERNAME': Variable.get('STATS_DB_USERNAME'),
            'STATS_DB_PASSWORD': Variable.get('STATS_DB_PASSWORD'),
            'DATA_RETENTION_DURATION_MONTHS': Variable.get('DATA_RETENTION_DURATION_MONTHS',
                                                           72)
            }
    return {k: v for k, v in envs.items() if v is not None}


def get_connection(name):
    return PostgresHook(postgres_conn_id=name).get_conn()
