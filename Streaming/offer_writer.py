import json
import os
import time
import logging

import psycopg2
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
OFFER_TOPIC = os.getenv("OFFER_TOPIC", "crm.realtime_offers")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASS", "airflow")
DB_NAME = os.getenv("DB_NAME", "airflow")


def get_conn(retries: int = 10, retry_delay: int = 3):
    """Connect to Postgres with retry logic."""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASS,
                dbname=DB_NAME,
            )
            logger.info(f"✓ Connected to Postgres at {DB_HOST} on attempt {attempt + 1}")
            return conn
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"✗ Postgres connection failed (attempt {attempt + 1}/{retries}): {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"✗ Postgres connection failed after {retries} attempts.")
                raise


def ensure_table(conn):
    ddl = """
    CREATE TABLE IF NOT EXISTS public.realtime_offers (
        id BIGSERIAL PRIMARY KEY,
        customer_id VARCHAR(255) NOT NULL,
        product VARCHAR(255) NOT NULL,
        base_score DOUBLE PRECISION,
        intent_score DOUBLE PRECISION,
        final_score DOUBLE PRECISION,
        threshold DOUBLE PRECISION,
        trigger_event VARCHAR(255),
        txn_10m INT,
        txn_2h INT,
        txn_24h INT,
        source VARCHAR(255),
        event_ts_ms BIGINT,
        payload JSONB,
        received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_realtime_offers_customer
        ON public.realtime_offers(customer_id);

    CREATE INDEX IF NOT EXISTS idx_realtime_offers_product
        ON public.realtime_offers(product);

    CREATE INDEX IF NOT EXISTS idx_realtime_offers_received_at
        ON public.realtime_offers(received_at DESC);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def insert_offer(conn, offer):
    txf = offer.get("txn_features", {})
    sql = """
    INSERT INTO public.realtime_offers (
        customer_id,
        product,
        base_score,
        intent_score,
        final_score,
        threshold,
        trigger_event,
        txn_10m,
        txn_2h,
        txn_24h,
        source,
        event_ts_ms,
        payload
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    """
    values = (
        offer.get("customer_id"),
        offer.get("product"),
        offer.get("base_score"),
        offer.get("intent_score"),
        offer.get("final_score"),
        offer.get("threshold"),
        offer.get("trigger_event"),
        txf.get("txn_10m"),
        txf.get("txn_2h"),
        txf.get("txn_24h"),
        offer.get("source"),
        offer.get("event_ts_ms"),
        json.dumps(offer),
    )
    with conn.cursor() as cur:
        cur.execute(sql, values)


def create_consumer(retries: int = 10, retry_delay: int = 3):
    """Create Kafka consumer with retry logic."""
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                OFFER_TOPIC,
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="realtime-offer-writer",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                request_timeout_ms=30000,
                session_timeout_ms=10000,
            )
            logger.info(f"✓ Connected to Kafka and subscribed to {OFFER_TOPIC} on attempt {attempt + 1}")
            return consumer
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"✗ Kafka connection failed (attempt {attempt + 1}/{retries}): {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"✗ Kafka connection failed after {retries} attempts.")
                raise


def main():
    while True:
        try:
            conn = get_conn()
            ensure_table(conn)
            logger.info("✓ Database table ensured")
            
            consumer = create_consumer()
            logger.info(f"✓ Offer writer subscribed to {OFFER_TOPIC}")

            for message in consumer:
                offer = message.value
                insert_offer(conn, offer)
                conn.commit()
                logger.info(
                    f"Stored offer: customer={offer.get('customer_id')} "
                    f"product={offer.get('product')} final_score={offer.get('final_score')}"
                )

        except Exception as ex:
            logger.error(f"Offer writer error: {ex}. Reconnecting in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    main()
