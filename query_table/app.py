import inspect
import json
import os
from datetime import datetime, timezone

import boto3
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from dap.api import DAPClient
from dap.dap_error import ProcessingError
from dap.dap_types import (CompleteIncrementalJob, CompleteJob,
                           CompleteSnapshotJob, Format, IncrementalQuery, Job,
                           JobStatus, SnapshotQuery, TableJob)
from strong_typing.exception import JsonKeyError
from strong_typing.serialization import (json_dump_string, json_to_object,
                                         object_to_json)

region = os.environ.get('AWS_REGION')
logger = Logger()

config = Config(region_name=region)
ssm_provider = parameters.SSMProvider(config=config)

ddb = boto3.resource('dynamodb')
ddb_table_name = os.environ.get('DDB_TABLE_NAME')
ddb_table = ddb.Table(ddb_table_name)

sqs = boto3.resource('sqs')
query_table_queue_url = os.environ.get('QUERY_TABLE_QUEUE_URL')
query_table_queue = sqs.Queue(query_table_queue_url)

job_status_queue_url = os.environ.get('JOB_STATUS_QUEUE_URL')
job_status_queue = sqs.Queue(job_status_queue_url)

namespace = 'canvas'
env = os.environ.get('ENV', 'dev')
file_format = Format(os.environ.get('FILE_FORMAT', 'csv'))

api_key_param_path = os.environ.get('API_KEY_PARAM_PATH', f'/{env}/canvas_data_2/dap_api_key')
api_base_url = os.environ.get('API_BASE_URL', 'https://api-gateway.instructure.com')

metrics = Metrics()
metrics.set_default_dimensions(environment=env)


@metrics.log_metrics
@logger.inject_lambda_context(log_event=True)
@event_source(data_class=SQSEvent)
def lambda_handler(event: SQSEvent, context: LambdaContext):

    dap_api_key = ssm_provider.get(api_key_param_path, max_age=600, decrypt=True)

    with DAPClient(base_url=api_base_url, api_key=dap_api_key) as dc:

        for record in event.records:
            table = record.body
            logger.info(f'going to query data for {table}')

            hwm, prior_job_id = get_table_high_water_mark(table)
            job = None
            job_type = None
            try:
                if hwm:
                    q = IncrementalQuery(format=file_format, filter=None, since=hwm, until=None)
                    job_type = 'incremental'
                    job = dc.query_incremental(namespace, table, q)
                else:
                    q = SnapshotQuery(format=file_format, filter=None)
                    job_type = 'snapshot'
                    job = dc.query_snapshot(namespace, table, q)
            except JsonKeyError as e:
                logger.warning(f'working around dap bug - table: {table}')
                # Try to work around a bug in the dap library: if the execute_job call
                # returns a completed job immediately, the library will fail to deserialize
                # the response data. Here we'll try to deserialize it as a CompleteSnapshotJob.
                # TODO: figure out how to recognize when it should be a CompleteIncrementalJob instead.
                response_payload = inspect.trace()[-1][0].f_locals
                job_attrs = response_payload['data'] | response_payload['field_values']
                if job_attrs.get('at'):
                    job = json_to_object(CompleteSnapshotJob, job_attrs)
                    job_type = 'snapshot'
                elif job_attrs.get('since'):
                    job = json_to_object(CompleteIncrementalJob, job_attrs)
                    job_type = 'incremental'
            except ProcessingError as e:
                if e.message.startswith('There is no data available yet'):
                    logger.info(e.message)
                else:
                    logger.exception(f'failed to query table {table}, format {file_format}, type {job_type}: {e.message}')
                    raise e

            if job:
                if job.id == prior_job_id:
                    logger.info(f'table query returned the same job ID as the prior job - no updates')
                else:
                    message_body = {
                        'table': table,
                        'job_id': job.id,
                        'job_type': job_type,
                        'file_format': file_format.value,
                    }
                    logger.info(f'initiated query job for table {table}: {job.id} ({job_type})')

                    result = job_status_queue.send_message(
                        MessageBody=json.dumps(message_body)
                    )

                    metrics.add_metric(name=f'{job_type}_job', unit=MetricUnit.Count, value=1)

                    logger.info(f'queued job_status job: {result}')

            query_message = sqs.Message(query_table_queue_url, record.receipt_handle)
            query_message.delete()
            logger.info('deleted the query_table message')


def get_table_high_water_mark(table_name: str):
    hwm = job_id = None
    result = ddb_table.query(
        KeyConditionExpression=Key('table_name').eq(table_name),
        IndexName='at-index',
        ScanIndexForward=False,
        Limit=1
    )

    logger.debug(result)
    if result.get('Count'):
        job = result['Items'][0]
        if job.get('at'):
            # convert milliseconds to decimal seconds and then to datetime
            at = job.get('at')
            hwm = datetime.fromtimestamp(at, tz=timezone.utc)
            job_id = job.get('id')
            logger.info(f'get hwm: sec: {at} dt: {hwm}')

    return (hwm, job_id)