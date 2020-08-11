from tools.utils.file_util import append_date_to_path
from tools.config.config import config

from os import path



def get_exports_bucket_name():
    return "exports-api"

def invalid_data_dir_name():
    return "invalid-data"


def get_s3_invalid_data_subfolder_path():
    return append_date_to_path(path.join(config['env'],
                                        invalid_data_dir_name())
                               )
