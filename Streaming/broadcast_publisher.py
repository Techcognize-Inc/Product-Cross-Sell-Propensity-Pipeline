import json
import os
import time
import psycopg2
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC = os.getenv('BROADCAST_TOPIC', 'propensity_scores.broadcast')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASS = os.getenv('DB_PASS', 'airflow')
DB_NAME = os.getenv('DB_NAME', 'airflow')
PUBLISH_INTERVAL_SECONDS = int(os.getenv('BROADCAST_INTERVAL_SECONDS', '60'))
RUN_ONCE = os.getenv('BROADCAST_RUN_ONCE', 'false').lower() == 'true'


def create_producer(retries=10, retry_delay=3):
    """Create Kafka producer with retry logic."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                key_serializer=lambda k: k.encode('utf-8'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000,
            )
            logger.info(f"✓ Kafka producer connected on attempt {attempt + 1}")
            return producer
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"✗ Kafka connection failed (attempt {attempt + 1}/{retries}): {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"✗ Kafka connection failed after {retries} attempts. Giving up.")
                raise


def fetch_propensity_scores():
    """Fetch latest propensity scores from Postgres."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME,
        )
        logger.info(f"Connected to Postgres at {DB_HOST}")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT customer_id, product_family, propensity_score, score_ts
            FROM public.mart_propensity_scores
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        logger.info(f"Fetched {len(rows)} propensity score records from database")

        scores = {}
        for row in rows:
            customer_id, product_family, score, ts = row
            if customer_id not in scores:
                scores[customer_id] = {
                    'customer_id': customer_id,
                    'scores': {},
                    'score_ts': ts.isoformat(),
                }
            scores[customer_id]['scores'][product_family] = score

        return list(scores.values())
    except Exception as e:
        logger.error(f"Error connecting to Postgres: {e}")
        raise


def publish_scores_once(producer):
    published = 0
    try:
        scores = fetch_propensity_scores()
        logger.info(f"Fetched {len(scores)} customer score snapshots from database")
        for score in scores:
            producer.send(TOPIC, key=score['customer_id'], value=score)
            published += 1
        producer.flush()
        return published
    except Exception as e:
        logger.error(f"Error fetching/publishing scores: {e}")
        raise


def publish_scores_forever():
    """Continuously publish propensity scores to Kafka every PUBLISH_INTERVAL_SECONDS."""
    producer = create_producer()
    iteration = 0
    while True:
        try:
            iteration += 1
            count = publish_scores_once(producer)
            logger.info(f"[{iteration}] Published {count} propensity snapshots to topic {TOPIC}")
            time.sleep(PUBLISH_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Error publishing scores: {e}. Reconnecting...")
            try:
                producer.close()
            except:
                pass
            producer = create_producer()


def publish_scores():
    """One-shot publish of scores (backward compatible entrypoint)."""
    producer = create_producer()
    try:
        count = publish_scores_once(producer)
        logger.info(f"Published {count} propensity snapshots to topic {TOPIC}")
        producer.flush()
    finally:
        producer.close()


if __name__ == '__main__':
    if RUN_ONCE:
        publish_scores()
    else:
        publish_scores_forever()
