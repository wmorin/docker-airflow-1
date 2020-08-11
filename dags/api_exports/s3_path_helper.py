from airflow.models import Variable
from tools.utils.file_util import append_date_to_path

from os import path

ENV = Variable.get('ENVIRONMENT')


def get_exports_bucket_name():
    return 'exports-api'

def invalid_data_dir_name():
    return 'invalid-data'


def get_s3_invalid_data_subfolder_path():
    return append_date_to_path(path.join(ENV,
                                        invalid_data_dir_name())
                               )
