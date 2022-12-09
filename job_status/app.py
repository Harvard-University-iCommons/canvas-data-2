import json
import os
from datetime import datetime
from typing import List

import boto3
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.config import Config
from dap.api import DAPClient
from dap.dap_types import (CompleteIncrementalJob, CompleteJob,
                           CompleteSnapshotJob, JobStatus, Resource)
from strong_typing.serialization import object_to_json

region = os.environ.get('AWS_REGION')
logger = Logger()

config = Config(region_name=region)
ssm_provider = parameters.SSMProvider(config=config)

ddb = boto3.resource('dynamodb')
ddb_table_name = os.environ.get('DDB_TABLE_NAME')
ddb_table = ddb.Table(ddb_table_name)

sqs = boto3.resource('sqs')
job_status_queue_url = os.environ.get('JOB_STATUS_QUEUE_URL')
job_status_queue = sqs.Queue(job_status_queue_url)

fetch_objects_queue_url = os.environ.get('FETCH_OBJECTS_QUEUE_URL')
fetch_objects_queue = sqs.Queue(fetch_objects_queue_url)

namespace = 'canvas'
env = os.environ.get('ENV', 'dev')

api_key_param_path = os.environ.get('API_KEY_PARAM_PATH', f'/{env}/canvas_data_2/dap_api_key')
api_base_url = os.environ.get('API_BASE_URL', 'https://api-gateway.instructure.com')

@logger.inject_lambda_context(log_event=True)
@event_source(data_class=SQSEvent)
def lambda_handler(event: SQSEvent, context: LambdaContext):

    dap_api_key = ssm_provider.get(api_key_param_path, max_age=600, decrypt=True)

    with DAPClient(base_url=api_base_url, api_key=dap_api_key) as dc:

        for record in event.records:
            message = json.loads(record.body)
            table = message['table']
            job_id = message['job_id']
            job_type = message['job_type']
            file_format = message['file_format']

            logger.info(f'going to check job status: {message}', extra=message)
            status = dc.get_job_status(job_id)
            logger.info(f'job status: {object_to_json(status)}')

            job_status_message = sqs.Message(job_status_queue_url, record.receipt_handle)

            if status.isTerminal():
                job = dc.get_job(job_id)

                if status == JobStatus.Complete:

                    ddb_result = put_job(table=table, job=job, file_format=file_format)
                    logger.info(ddb_result)

                    resources: List[Resource] = dc.get_resources(job.objects)
                    for r in resources:
                        message['url'] = str(r.url)

                        fetch_objects_queue.send_message(
                            MessageBody=json.dumps(message)
                        )

                    logger.info(f'job {job_id} (table {table}) is complete - sent to fetch_objects queue', extra={'table': table, 'status': str(status)})
                else:
                    # something went wrong
                    logger.error(f'table query for {table}, type {job_type}, file_format {file_format} failed: {job.error}', extra=message)

            else:

                # put a new job_status message in the queue - check again in a bit
                job_status_queue.send_message(
                    MessageBody=json.dumps(message)
                )
                logger.info(f'job {job_id} (table {table}) not complete - requeued', extra={'table': table, 'status': str(status)})

            # remove the original job_status message from the queue
            job_status_message.delete()


def put_job(table: str, job, file_format: str):

    item = {
        'id': job.id,
        'status': str(job.status),
        'file_format':  file_format,
    }
    item['table_name'] = table
    if issubclass(type(job), CompleteJob):
        simple_objects = []
        objects = job.objects
        for o in objects:
            simple_objects.append(o.id)
        if job.expires_at:
            item['expires_at'] = job.expires_at.isoformat()
        item['objects'] = simple_objects
        item['schema_version'] = job.schema_version
    if type(job) is CompleteSnapshotJob:
        item['job_type'] = 'snapshot'
        # convert to an integer (milliseconds) that DynamoDB can store
        at = job.at
        ts = datetime.timestamp(at)
        logger.debug(f'at: {at} ts: {ts}')
        item['at'] = int(ts)
    elif type(job) is CompleteIncrementalJob:
        item['job_type'] = 'incremental'
        # convert to an integer (milliseconds) that DynamoDB can store
        at = job.until
        ts = datetime.timestamp(at)
        logger.debug(f'at: {at} ts: {ts}')
        item['at'] = int(ts)
        item['since'] =  job.since.isoformat()
        item['until'] = job.until.isoformat()

    response = ddb_table.put_item(
        Item=item
    )
    logger.debug(response)
