import os

import boto3
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from dap.api import DAPClient
from dap.dap_error import ProcessingError
from dap.dap_types import (CompleteIncrementalJob, CompleteJob,
                           CompleteSnapshotJob, Format, IncrementalQuery, Job,
                           JobStatus, SnapshotQuery, TableJob)
from strong_typing.exception import JsonKeyError
from strong_typing.serialization import (json_dump_string, json_to_object,
                                         object_to_json)
from botocore.config import Config


region = os.environ.get('AWS_REGION')

config = Config(region_name=region)
ssm_provider = parameters.SSMProvider(config=config)

logger = Logger()
ddb = boto3.resource('dynamodb')
ddb_table_name = os.environ.get('DDB_TABLE_NAME')
ddb_table = ddb.Table(ddb_table_name)

sqs = boto3.resource('sqs')
query_table_queue_url = os.environ.get('QUERY_TABLE_QUEUE_URL')
query_table_queue = sqs.Queue(query_table_queue_url)

env = os.environ.get('ENV', 'dev')

metrics = Metrics()
metrics.set_default_dimensions(environment=env)

namespace = 'canvas'

@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context: LambdaContext):

    params = ssm_provider.get_multiple(f'/{env}/canvas_data_2', max_age=300, decrypt=True)


    with DAPClient(base_url=params['dap_api_url'], api_key=params['dap_api_key']) as dc:
        tables = dc.get_tables(namespace)
        count = 0
        for table in tables:
            result = query_table_queue.send_message(
                MessageBody=table,
            )
            logger.info(f'queued query_table job for table {table}: {result}')
            count += 1
            # if count > 5:
            #     break
