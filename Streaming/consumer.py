import argparse
import json
import os
import time
import logging
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')


def create_consumer(topic: str, group_id: str = 'streaming-consumer', retries: int = 10, retry_delay: int = 3) -> KafkaConsumer:
    """Create Kafka consumer with retry logic."""
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=group_id,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                request_timeout_ms=5000,
                session_timeout_ms=10000,
            )
            logger.info(f"✓ Connected to Kafka broker {BOOTSTRAP_SERVERS} on attempt {attempt + 1}")
            return consumer
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"✗ Kafka connection failed (attempt {attempt + 1}/{retries}): {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"✗ Kafka connection failed after {retries} attempts. Giving up.")
                raise


def consume_transactions(topic: str = 'txn.stream'):
    logger.info(f'Attempting to subscribe to topic: {topic}')
    consumer = create_consumer(topic=topic, group_id='txn-stream-group')
    logger.info(f'✓ Successfully subscribed to topic: {topic}')

    try:
        for message in consumer:
            event = message.value
            logger.info(f'Received event from {topic}: customer={event.get("customer_id")}, product={event.get("product")}')
    except KeyboardInterrupt:
        logger.info('Consumer interrupted by user')
    except Exception as e:
        logger.error(f'Error consuming messages: {e}')
    finally:
        consumer.close()
        logger.info('Consumer closed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Consume Kafka events from a topic')
    parser.add_argument('--topic', type=str, default='txn.stream')
    args = parser.parse_args()
    consume_transactions(args.topic)
