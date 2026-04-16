import argparse
import json
import os
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')


def create_consumer(topic: str, group_id: str = 'streaming-consumer') -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )


def consume_transactions(topic: str = 'txn.stream'):
    consumer = create_consumer(topic=topic, group_id='txn-stream-group')
    print(f'Subscribed to topic: {topic}')

    for message in consumer:
        event = message.value
        print(f'Received event from {topic}: {event}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Consume Kafka events from a topic')
    parser.add_argument('--topic', type=str, default='txn.stream')
    args = parser.parse_args()
    consume_transactions(args.topic)
