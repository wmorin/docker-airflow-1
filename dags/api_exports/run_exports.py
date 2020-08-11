
from tools.api_exports.agents import fetch_agents
from tools.api_exports.customers import fetch_customers
from tools.api_exports.conversations import fetch_conversations
from tools.utils.aws_util import s3_upload_file
from tools.utils.time_util import get_n_days_from_now_string, get_current_utc_string, get_localized_date
from aiqdynamo.tables.agents import AgentsTable
from aiqdynamo.tables.customers import CustomersTable
from aiqdynamo.tables.conversations import ConversationsTable
from tools.utils.file_util import append_date_to_path
from tools.utils.db_util import connect_core_db
from .s3_path_helper import get_exports_bucket_name
from .exports_data_validator import dynamoRecordsValidator
import time
import json
import argparse
import logging

EXPORT_BUCKET_NAME = get_exports_bucket_name()
DEFAULT_SPLIT_SIZE = 25
DEFAULT_DELTHA_DAYS = 2

# validate non changing fields that are
# common to both core db and dynamodb
AGENT_VALIDATION_FIELDS = ['id']
CUSTOMER_VALIDATION_FIELDS = ['id', 'primary_agent']
CONVERSATIONS_VALIDATION_FIELDS = ['id', 'customer_id']

def split_into_batches(records, batch_split_size):
    if not records:
        return []
    for i in range(0, len(records), batch_split_size):
        yield records[i:i + batch_split_size]


def batch_write(file_path, batch_func, batch_split_size=None):

    if not batch_split_size:
        batch_split_size = DEFAULT_SPLIT_SIZE

    with open(file_path, 'r') as records_file:
        records = json.load(records_file)

        for batch in split_into_batches(records, batch_split_size):
            batch_func(batch)
            time.sleep(2)


def save_to_s3(exports_file_path, env):
    bucket = EXPORT_BUCKET_NAME
    sub_path = append_date_to_path(env)
    s3_upload_file(bucket,
                   exports_file_path,
                   sub_path)
    print(f"{exports_file_path} uploaded to {bucket}/{sub_path} ")


def export_conversations_to_dynamo(start_date, end_date, env):
    conversations_file_path = fetch_conversations(start_date, end_date)
    save_to_s3(conversations_file_path, env)
    batch_write(conversations_file_path, ConversationsTable.batch_write_conversation_records,
                DEFAULT_SPLIT_SIZE)


def export_customers_to_dynamo(start_date, end_date, env):
    customers_file_path = fetch_customers(start_date, end_date)
    save_to_s3(customers_file_path, env)
    batch_write(customers_file_path, CustomersTable.batch_write_customer_records)


def export_agents_to_dynamo(start_date, end_date, env):
    agents_file_path = fetch_agents(start_date, end_date)
    save_to_s3(agents_file_path, env)
    batch_write(agents_file_path, AgentsTable.batch_write_agent_records)



def process_dates(start_date, end_date):
    if not end_date:
        end_date = get_current_utc_string()

    if not start_date:
        start_date = get_n_days_from_now_string(DEFAULT_DELTHA_DAYS,
                                                past=True)

    return (get_localized_date(start_date),
             get_localized_date(end_date))


def run_exports(start_date=None, end_date=None, env=None, bucket=EXPORT_BUCKET_NAME):

    start_date, end_date = process_dates(start_date, end_date)
    print('Started uploading agents to dynamo')
    export_agents_to_dynamo(start_date, end_date, env)
    print('Finished uploading agents to dynamo')

    print('Started uploading customers to dynamo')
    export_customers_to_dynamo(start_date, end_date, env)
    print('Finished uploading customers to dynamo')

    print('Started uploading conversations to dynamo')
    export_conversations_to_dynamo(start_date, end_date, env)
    print('Finished uploading conversations to dynamo')


def add_id(conversation_json):
    conversation_json['id'] = conversation_json['conversation_id']
    return conversation_json


def validate_exports(start_date=None, end_date=None):
    core_db_conn = connect_core_db()

    def validate_agents(validation_fields, start_date, end_date, coredb_id_field='id'):
        print(f'Validating  agents from dynamo from {start_date} to {end_date}')
        agents_validator = dynamoRecordsValidator('agents', validation_fields, core_db_conn)
        agents_validator.validate(AgentsTable.get_agent_records(start_date=start_date, end_date=end_date),
                                  coredb_id_field)
        print('Finished validating agents from dynamo')


    def validate_customers(validation_fields, start_date, end_date, coredb_id_field='id'):
        print(f'Validating  customers from dynamo from {start_date} to {end_date}')
        customers_validator = dynamoRecordsValidator('customers', validation_fields, core_db_conn)
        customers_validator.validate(CustomersTable.get_customer_records(start_date=start_date, end_date=end_date),
                                     coredb_id_field)
        print('Finished validating customers from dynamo')


    def validate_conversations(validation_fields, start_date, end_date, coredb_id_field='id'):
        print(f'Validating  conversations from dynamo from {start_date} to {end_date}')
        conversations_validator = dynamoRecordsValidator('conversations', validation_fields, core_db_conn)
        conversations = ConversationsTable.get_conversation_records(start_date=start_date, end_date=end_date)
        conversations = list(map(add_id, conversations))
        # For other tables and even this table all column names are same
        # between core db and dynamo with the exception of conversation_id
        # in dynamo's conversation table which is called id in core db's table
        # the above line accounts for this by adding id field to each row
        conversations_validator.validate(conversations, coredb_id_field)
        print('Finished validating  conversations from dynamo')
    print('started validation')
    start_date, end_date = process_dates(start_date, end_date)
    start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
    validate_agents(AGENT_VALIDATION_FIELDS, start_date, end_date)
    validate_customers(CUSTOMER_VALIDATION_FIELDS, start_date, end_date)
    validate_conversations(CONVERSATIONS_VALIDATION_FIELDS, start_date, end_date)
    core_db_conn.close()
    print('finished validation')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, help='Start date - format YYYY-MM-DD %H:%m:%s', default=None)
    parser.add_argument('--end_date', type=str, help='End date - format YYYY-MM-DD %H:%m:%s', default=None)
    parser.add_argument('--s3_bucket',
                        type=str,
                        help='S3 bucket name to temporarily store intermediate data',
                        default=EXPORT_BUCKET_NAME)
    parser.add_argument('--export',
                        action = 'store_true',
                        help='push data from analytics and core db to dynamo',
                        default=False)

    parser.add_argument('--validate',
                        action='store_true',
                        help='pull data from dynamo and validate against s3',
                        default=False)

    args = parser.parse_args()
    if args.export:
        logging.info('running exports..')
        run_exports(args.start_date, args.end_date, 'demo4', args.s3_bucket)
    if args.validate:
        logging.info('running validation..')
        validate_exports(args.start_date, args.end_date)
