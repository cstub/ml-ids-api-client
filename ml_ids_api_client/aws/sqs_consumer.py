"""
Provides a Amazon SQS Consumer component
"""
from typing import List
import logging
import boto3
from botocore.exceptions import ClientError


class AwsSQSConsumer:
    """
    AWS SQS Message consumer
    """
    KEY_MESSAGES = 'Messages'
    KEY_RECEIPT_HANDLE = 'ReceiptHandle'

    def __init__(self, access_key: str, secret_key: str, region: str) -> None:
        self.client = boto3.client('sqs',
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key,
                                   region_name=region)

    def receive_messages(self,
                         queue_url: str,
                         num_messages: int,
                         wait_time_seconds: int,
                         visibility_timeout: int) -> List[dict]:
        """
        Polls the message queue and returns visible messages.

        :param queue_url: URL of the message queue.
        :param num_messages: Number of messages to receive per polling interval. Should be between 1 - 10.
        :param wait_time_seconds: Time to wait in seconds for each polling request to return if no messages are
        available. A value greater 0 activates the use of long-polling.
        :param visibility_timeout: Visibility timeout for a delivered messages.
        :return: List of messages.
        """
        try:
            response = self.client.receive_message(QueueUrl=queue_url,
                                                   MaxNumberOfMessages=num_messages,
                                                   WaitTimeSeconds=wait_time_seconds,
                                                   VisibilityTimeout=visibility_timeout)

            return response[AwsSQSConsumer.KEY_MESSAGES] if AwsSQSConsumer.KEY_MESSAGES in response else []
        except ClientError as err:
            logging.error('AWS Client error occurred while receiving messages: %s', err)
            return []

    def delete_messages(self, queue_url: str, messages: List[dict]) -> None:
        """
        Deletes the given message from the message queue.

        :param queue_url: URL of the message queue.
        :param messages: Messages to delete.
        :return: None
        """
        for msg in messages:
            if AwsSQSConsumer.KEY_RECEIPT_HANDLE not in msg:
                logging.warning('Message [%s] could not be deleted as it contains no receipt-handle', msg)
                continue

            try:
                self.client.delete_message(QueueUrl=queue_url,
                                           ReceiptHandle=msg[AwsSQSConsumer.KEY_RECEIPT_HANDLE])
            except ClientError as err:
                logging.error('AWS Client error occurred while deleting message [%s]: %s', msg, err)
