import json
import os
import psycopg2
from kafka import KafkaProducer

TOPIC = os.getenv('BROADCAST_TOPIC', 'propensity_scores.broadcast')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASS = os.getenv('DB_PASS', 'airflow')
DB_NAME = os.getenv('DB_NAME', 'airflow')


def create_producer():
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )


def fetch_propensity_scores():
    conn = psycopg2.connect(
        host=postgres,
        user=airflow,
        password=airflow,
        dbname=airflow,
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT customer_id, product_family, propensity_score, score_ts
        FROM mart_propensity_scores
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    scores = {}
    for row in rows:
        customer_id, product_family, score, ts = row
        if customer_id not in scores:
            scores[customer_id] = {'scores': {}, 'score_ts': ts.isoformat()}
        scores[customer_id]['scores'][product_family] = score

    return list(scores.values())


def publish_scores():
    producer = create_producer()
    scores = fetch_propensity_scores()
    for score in scores:
        producer.send(TOPIC, score)
        print(f'Published broadcast score for {score}')
    producer.flush()
    producer.close()


if __name__ == '__main__':
    publish_scores()
