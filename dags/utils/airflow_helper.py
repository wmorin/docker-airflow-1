from airflow.models import Variable


def get_environments():
    return {'ENVIRONMENT': Variable.get('ENVIRONMENT'),
            'BASE_API_DB_PORT':  Variable.get('BASE_API_DB_PORT'),
            'BASE_API_DB_HOST': Variable.get('BASE_API_DB_HOST'),
            'BASE_API_DB_NAME': Variable.get('BASE_API_DB_NAME'),
            'BASE_API_DB_USERNAME': Variable.get('BASE_API_DB_USERNAME'),
            'BASE_API_DB_PASSWORD': Variable.get('BASE_API_DB_PASSWORD'),
            'ANALYTICS_DB_PORT': Variable.get('ANALYTICS_DB_PORT'),
            'ANALYTICS_DB_HOST': Variable.get('ANALYTICS_DB_HOST'),
            'ANALYTICS_DB_NAME': Variable.get('ANALYTICS_DB_NAME'),
            'ANALYTICS_DB_USERNAME': Variable.get('ANALYTICS_DB_USERNAME'),
            'ANALYTICS_DB_PASSWORD': Variable.get('ANALYTICS_DB_PASSWORD'),
            'STATS_DB_PORT': Variable.get('STATS_DB_PORT'),
            'STATS_DB_HOST': Variable.get('STATS_DB_HOST'),
            'STATS_DB_NAME': Variable.get('STATS_DB_NAME'),
            'STATS_DB_USERNAME': Variable.get('STATS_DB_USERNAME'),
            'STATS_DB_PASSWORD': Variable.get('STATS_DB_PASSWORD')}
