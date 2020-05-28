"""
    AWS helpers and constants
"""


def create_etl_daily_key_prefix(date):
    return f'etl/daily/{date}'
