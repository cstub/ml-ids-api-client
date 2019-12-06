import logging
import pandas as pd

import click
import json

from collections import namedtuple
from ml_ids_api_client.aws.sqs_consumer import AwsSQSConsumer
from ml_ids_api_client.user_interaction import print_dataframe

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(asctime)s: %(message)s')

AttackNotification = namedtuple('AttackNotification', ['msg_id', 'network_flow'])


def deserialize_message(message):
    try:
        msg_id = message['MessageId']
        msg_body = json.loads(message['Body'])
        msg_content = msg_body['Message']

        network_flow = pd.read_json(msg_content, orient='split')
        return AttackNotification(msg_id, network_flow)
    except KeyError as err:
        logging.warning("Message [{}] could not be parsed. Cause: {}".format(message, err))
    except ValueError as err:
        logging.warning('Message content [{}] could not be parsed. Message does not adhere to Pandas-split format. '
                        'Cause {}'.format(msg_content, err))
    return None


def print_attack_notification(attack_notification, display_overflow):
    print('Attack detected [{}]'.format(attack_notification.msg_id))
    print('Network Flow:\n')
    print_dataframe(attack_notification.network_flow, display_overflow)
    print('\n')


@click.command()
@click.option('--access-key', type=str, required=True,
              help='Access key of the AWS account connecting to the SQS queue.')
@click.option('--secret-key', type=str, required=True,
              help='Secret key of the AWS account connecting to the SQS queue.')
@click.option('--region', type=str, required=True,
              help='AWS region.')
@click.option('--queue-url', type=str, required=True,
              help='AWS SQS queue URL.')
@click.option('--delete-messages', type=bool, default=False,
              help='Whether received messages should be deleted or preserved.')
@click.option('--num-messages', type=click.IntRange(1, 10), default=10,
              help='Number of messages requested per polling request.')
@click.option('--wait-time', type=click.IntRange(0, (60 * 60)), default=10,
              help='Time to wait in seconds for each polling request to return if no messages are available. '
                   'Specify a value greater 0 to use long-polling.')
@click.option('--visibility-timeout', type=click.IntRange(0, (60 * 60)), default=60,
              help='Visibility timeout for a delivered message.')
@click.option('--display-overflow', type=click.Choice(['WRAP', 'NOWRAP'], case_sensitive=False),
              default='WRAP', help='Defines the overflow behaviour if the output exceeds the window width.')
def run_cli(access_key,
            secret_key,
            region,
            queue_url,
            delete_messages,
            num_messages,
            wait_time,
            visibility_timeout,
            display_overflow):
    sqs_consumer = AwsSQSConsumer(access_key, secret_key, region)

    while True:
        logging.info("Retrieving messages from queue...")
        messages = sqs_consumer.receive_messages(queue_url, num_messages, wait_time, visibility_timeout)

        for message in messages:
            attack_notification = deserialize_message(message)
            print_attack_notification(attack_notification, display_overflow)

        if delete_messages:
            sqs_consumer.delete_messages(queue_url, messages)


if __name__ == '__main__':
    run_cli()
