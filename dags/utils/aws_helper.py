"""
    AWS helpers and constants
"""
import os


def make_s3_key(env, subdir, date_string=None, event_period='daily', file_name=None):
    """ Make s3 bucket path to
    /{env}/{event_name}/(daily|monthly|etc..)/date_string(/filename)"""
    if file_name:
        return os.path.join(env, subdir, event_period, date_string, file_name)
    else:
        if date_string:
            return os.path.join(env, subdir, event_period, date_string)
        else:
            return os.path.join(env, subdir, event_period)
