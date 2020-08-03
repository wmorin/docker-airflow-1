
from tools.api_exports.agents import fetch_agents
from tools.api_exports.customers import fetch_customers
from tools.api_exports.conversations import fetch_conversations
from tools.utils.aws_util import s3_upload_file
from tools.utils.time_util import get_n_days_from_now_string, get_current_utc_string, get_localized_date
from aiqdynamo.tables.agents import AgentsTable
from aiqdynamo.tables.customers import CustomersTable
from aiqdynamo.tables.conversations import ConversationsTable
from tools.utils.file_util import append_date_to_path

import time
import json
import argparse

EXPORT_BUCKET_NAME = 'exports-api'
DEFAULT_SPLIT_SIZE = 25
DEFAULT_DELTHA_DAYS = 2


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


def run_exports(start_date=None, end_date=None, env=None, bucket=EXPORT_BUCKET_NAME):

    if not end_date:
        end_date = get_current_utc_string()

    if not start_date:
        start_date = get_n_days_from_now_string(DEFAULT_DELTHA_DAYS,
                                                past=True)

    end_date = get_localized_date(end_date)
    start_date = get_localized_date(start_date)
    print('Started uploading agents to dynamo')
    export_agents_to_dynamo(start_date, end_date, env)
    print('Finished uploading agents to dynamo')

    print('Started uploading customers to dynamo')
    export_customers_to_dynamo(start_date, end_date, env)
    print('Finished uploading customers to dynamo')

    print('Started uploading conversations to dynamo')
    export_conversations_to_dynamo(start_date, end_date, env)
    print('Finished uploading conversations to dynamo')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, help='Start date - format YYYY-MM-DD %H:%m:%s', default=None)
    parser.add_argument('--end_date', type=str, help='End date - format YYYY-MM-DD %H:%m:%s', default=None)
    parser.add_argument('--s3_bucket',
                        type=str,
                        help='S3 bucket name to temporarily store intermediate data',
                        default=EXPORT_BUCKET_NAME)

    args = parser.parse_args()
    run_exports(args.start_date, args.end_date, 'demo4', args.s3_bucket)
