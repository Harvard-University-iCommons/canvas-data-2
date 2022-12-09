import json
import os
from urllib.parse import urlparse

import boto3
import requests
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.config import Config
from smart_open import open

region = os.environ.get('AWS_REGION')
logger = Logger()

config = Config(region_name=region)
ssm_provider = parameters.SSMProvider(config=config)

ddb = boto3.resource('dynamodb')
ddb_table_name = os.environ.get('DDB_TABLE_NAME')
ddb_table = ddb.Table(ddb_table_name)

sqs = boto3.resource('sqs')

fetch_objects_queue_url = os.environ.get('FETCH_OBJECTS_QUEUE_URL')
fetch_objects_queue = sqs.Queue(fetch_objects_queue_url)

s3 = boto3.resource('s3')
bucket_name = os.environ.get('BUCKET_NAME')
s3_bucket = s3.Bucket(bucket_name)

namespace = 'canvas'
env = os.environ.get('ENV', 'dev')

metrics = Metrics()
metrics.set_default_dimensions(environment=env)

chunk_size = 1024*1024*8


@metrics.log_metrics
@logger.inject_lambda_context(log_event=True)
@event_source(data_class=SQSEvent)
def lambda_handler(event: SQSEvent, context: LambdaContext):

    for record in event.records:
        message = json.loads(record.body)
        table = message['table']
        job_id = message['job_id']
        job_type = message['job_type']
        file_format = message['file_format']
        url = message['url']

        url_path = urlparse(url).path
        file_base_name = os.path.basename(url_path)
        file_key = f'{file_format}/{table}/{job_id}_{job_type}/{file_base_name}'

        with open(f's3://{bucket_name}/{file_key}', 'wb', compression='disable') as fout:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        fout.write(chunk)


        metrics.add_metric(name=f'file_downloaded', unit=MetricUnit.Count, value=1)

        logger.info(f'downloaded file for job {job_id} ({table}): {file_base_name}')

        fetch_objects_message = sqs.Message(fetch_objects_queue_url, record.receipt_handle)
        fetch_objects_message.delete()
