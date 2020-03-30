import os


class InputError(Exception):
    def __init__(self, message):
        self.message = message


def get_daily_weekly_script(frequency, e=os.environ):
    """ Generate daily/weekly analytics run """

    if not frequency or frequency not in ['daily', 'weekly']:
        raise InputError(f'frequency cannot be {frequency}')

    return f'ENVIRONMENT={e["ENVIRONMENT"]} python3 -m tools.analytics.simple_stats \
        --{frequency} --upload_to_s3 \
        --dbname={e["ANALYTICS_DB_NAME"]} --host={e["ANALYTICS_DB_HOST"]} \
        --port={e["ANALYTICS_DB_PORT"]} --user={e["ANALYTICS_DB_USERNAME"]} --password={e["ANALYTICS_DB_PASSWORD"]} \
        --core_dbname={e["BASE_API_DB_NAME"]} --core_host={e["BASE_API_DB_HOST"]} --core_port={e["BASE_API_DB_PORT"]} \
        --core_user={e["BASE_API_DB_USERNAME"]} --core_password={e["BASE_API_DB_PASSWORD"]}'
