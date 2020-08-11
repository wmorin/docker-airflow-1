import logging
from tools.utils.file_util import dump_to_csv_file
from tools.utils.file_util import remove_files
from .s3_path_helper import get_exports_bucket_name
from .s3_path_helper import get_s3_invalid_data_subfolder_path
from os import  path
from os import  getcwd
from tools.utils.aws_util import s3_upload_file



class dynamoRecordsValidator:
    def __init__(self, core_db_table_name, common_cols, core_conn):
        self._table_name = core_db_table_name
        self._common_cols = common_cols
        self._cursor = core_conn.cursor()
        self._dump_file_path = path.join(getcwd(),
                                         f"dynamo_db_core_db_discrepancies_for_{core_db_table_name}.csv")
        self._invalid_data = []


    def _get_query(self):
        return """select {} from {} where id = %s""".format(','.join(self._common_cols),
                                                            self._table_name
                                                            )
    def _save_invalid_records(self):
        if self._invalid_data:
            dump_to_csv_file(self._dump_file_path,
                             [f"{self._table_name}_table_id",
                              "column_name",
                              "dynamo_value",
                              "core_db_value"],
                             self._invalid_data)
            bucket = get_exports_bucket_name()
            sub_path = get_s3_invalid_data_subfolder_path()
            s3_upload_file(bucket,
                           self._dump_file_path,
                           sub_path)
            logging.info(f"{self._dump_file_path} uploaded to {bucket}/{sub_path} ")
            remove_files([self._dump_file_path])

    def _run_query(self, query, query_params):
        if not self._cursor:
            logging.error(f'cursor is undefined for the query {query}')
            return []
        self._cursor.execute(query, query_params)

    def _fetch_coredb_row(self, row_id):
        query = self._get_query()
        self._run_query(query, (row_id,))
        return self._cursor.fetchone()

    def _find_invalid_data(self, coredb_row, dynamo_row, row_id):
        for i, col in enumerate(self._common_cols):
            if coredb_row[i] != dynamo_row[i]:
                logging.error('mismatch :')
                logging.error(f"{coredb_row[i]} from coredb does not match"
                              f" {dynamo_row[i]} generated for dynamo for the col "
                              f"{col} of table {self._table_name}")
                self._invalid_data.append([row_id,  col, dynamo_row[i],  coredb_row[i]])

    def validate(self, dynamorecords, id_col):
        if not dynamorecords:
            logging.info('No dynamo records supplied for validation')
            return
        for dynamorow_json in dynamorecords:
            row_id = dynamorow_json[id_col]
            coredb_row = self._fetch_coredb_row(row_id)
            dynamo_row =  [dynamorow_json[k] for k in self._common_cols]
            if coredb_row:
                self._find_invalid_data(coredb_row, dynamo_row, row_id)
            else:
                logging.info('item not found in core db : ')
                logging.info(f"core db {self._table_name} returns nothing for {dynamorow_json}")
                #  These cases are expected to occur more often
                # because core db does not store all historical data
        self._save_invalid_records()
        return self._invalid_data == []

