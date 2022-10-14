from prettytable import PrettyTable

from azure_client.blob_storage import get_azure_message_counts
from constants import TOTAL, TEST_PASSED, TEST_FAILED
from kafka_client.consumer import get_topic_message_counts


def test_connector(topic, directory):
    """
    Method to test if all the produced messages are successfully consumed by the sink
    :param topic:
    :param directory:
    :return:
    """
    produced_message_counts = get_topic_message_counts(topic)
    if produced_message_counts[TOTAL] == 0:
        raise Exception('No messages to consume.')
    consumed_message_counts = get_azure_message_counts(f'{directory}/{topic}/')

    result = produced_message_counts[TOTAL] == consumed_message_counts[TOTAL]
    message = TEST_PASSED if result else TEST_FAILED

    print()
    print('####################################################################################################')

    table = PrettyTable(produced_message_counts.keys())
    table.add_row(produced_message_counts.values())
    print('Messages Produced:')
    print(table)

    table = PrettyTable(consumed_message_counts.keys())
    table.add_row(consumed_message_counts.values())
    print('Messages Consumed:')
    print(table)

    print('####################################################################################################')
    print(f'#                                         {message}                                         #')
    print('####################################################################################################')
    print()
