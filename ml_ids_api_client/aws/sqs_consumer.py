import logging
import boto3
from botocore.exceptions import ClientError


class AwsSQSConsumer(object):
    KEY_MESSAGES = 'Messages'
    KEY_RECEIPT_HANDLE = 'ReceiptHandle'

    def __init__(self, access_key, secret_key, region):
        self.client = boto3.client('sqs',
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key,
                                   region_name=region)

    def receive_messages(self, queue_url, num_messages, wait_time_seconds, visibility_timeout):
        try:
            response = self.client.receive_message(QueueUrl=queue_url,
                                                   MaxNumberOfMessages=num_messages,
                                                   WaitTimeSeconds=wait_time_seconds,
                                                   VisibilityTimeout=visibility_timeout)

            return response[AwsSQSConsumer.KEY_MESSAGES] if AwsSQSConsumer.KEY_MESSAGES in response else []
        except ClientError as err:
            logging.error('AWS Client error occurred while receiving messages: {}'.format(err))
            return []

    def delete_messages(self, queue_url, messages):
        for msg in messages:
            if AwsSQSConsumer.KEY_RECEIPT_HANDLE not in msg:
                logging.warning('Message [{}] could not be deleted as it contains no receipt-handle'.format(msg))
                continue

            try:
                self.client.delete_message(QueueUrl=queue_url,
                                           ReceiptHandle=msg[AwsSQSConsumer.KEY_RECEIPT_HANDLE])
            except ClientError as err:
                logging.error('AWS Client error occurred while deleting message [{}]: {}'.format(msg, err))
