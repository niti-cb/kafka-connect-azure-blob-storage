import os
from time import time, sleep

from kafka import KafkaConsumer, TopicPartition

from constants import TOTAL

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '').split(',')

def get_topic_partitions(consumer, topic):
    """
    Method to get all the partitions of the topic
    :param consumer:
    :param topic:
    :return:
    """
    partition_names = consumer.partitions_for_topic(topic)
    if not partition_names:
        raise Exception('Topic partitions not found.')
    partitions = [TopicPartition(topic, partition) for partition in partition_names]
    return partitions


def get_topic_message_counts(topic):
    """
    Method to get count of messages that are published to the topic (partition wise)
    :param topic:
    :return:
    """
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS, request_timeout_ms=30000)
    partitions = get_topic_partitions(consumer, topic)
    partition_offsets = consumer.end_offsets(partitions)
    consumer.close(autocommit=False)
    counts = {f'PARTITION={partition.partition}': count for partition, count in partition_offsets.items() if count}
    counts[TOTAL] = sum(counts.values())
    return counts


def wait_until_consumption_stops(topic, consumer_group, timeout=900):
    """
    Method to wait for the consumer_group to finish consuming all messages from given topic
    :param topic:
    :param consumer_group:
    :param timeout:
    :return:
    """
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS, group_id=consumer_group, request_timeout_ms=30000)
    partitions = get_topic_partitions(consumer, topic)

    offsets = {}
    start_time = time()

    while True:
        changed = False
        for partition in partitions:
            partition_name = f'PARTITION={partition.partition}'
            consumer.assign([partition])
            partition_offset = consumer.position(partition)
            if offsets.get(partition_name) == partition_offset:
                continue
            changed = True
            offsets[partition_name] = partition_offset
        print(f'Current Offsets: {offsets}')

        if not changed:
            break

        elapsed = time() - start_time
        if elapsed > timeout:
            raise Exception('Timed out')
        print('Sleeping for 1 minute...')
        sleep(60)

    consumer.close(autocommit=False)
