import json
import os
import random
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer

PRODUCT_CATEGORIES = [
    'home_improvement',
    'electronics',
    'travel',
    'retail',
    'insurance',
    'utilities',
    'salary',
]

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = os.getenv('TRANSACTION_TOPIC', 'txn.stream')
DATA_DIR = os.getenv('DATA_DIR', '/app/data')


def create_producer():
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )


def load_customers():
    try:
        path = os.path.join(DATA_DIR, 'demographics.parquet')
        return pd.read_parquet(path)['customer_id'].tolist()
    except Exception:
        return [f'cust_{i:06d}' for i in range(1, 1001)]


def load_holdings():
    try:
        path = os.path.join(DATA_DIR, 'product_holdings.parquet')
        holdings = pd.read_parquet(path)
        return {row['customer_id']: row.to_dict() for _, row in holdings.iterrows()}
    except Exception:
        return {}


def generate_transaction(customer_id: str, holdings: dict):
    category = random.choices(
        PRODUCT_CATEGORIES,
        weights=[0.12, 0.12, 0.10, 0.30, 0.08, 0.18, 0.10],
        k=1,
    )[0]
    if category == 'salary':
        amount = round(random.uniform(2000.0, 8000.0), 2)
        txn_type = 'salary'
    else:
        amount = round(random.uniform(15.0, 2500.0), 2)
        txn_type = 'debit'

    product = 'credit_card'
    trigger_signal = 'credit_card_intent'
    if category == 'home_improvement':
        product = 'home_loan'
        trigger_signal = 'home_loan_intent'
    elif category in ['electronics', 'travel']:
        product = 'personal_loan'
        trigger_signal = 'personal_loan_intent'
    elif category == 'salary':
        product = 'fixed_deposit'
        trigger_signal = 'fd_intent'

    holding = holdings.get(customer_id, {})
    if product == 'credit_card' and holding.get('has_credit_card', 0) == 1:
        product = 'home_loan'
    if product == 'fixed_deposit' and holding.get('has_fixed_deposit', 0) == 1:
        product = 'home_loan'

    return {
        'customer_id': customer_id,
        'event_ts': datetime.utcnow().isoformat(),
        'merchant_category': category,
        'transaction_type': txn_type,
        'amount': amount,
        'product': product,
        'trigger_signal': trigger_signal,
    }


def publish_transactions(interval_seconds=1):
    producer = create_producer()
    customers = load_customers()
    holdings = load_holdings()

    while True:
        customer_id = random.choice(customers)
        txn = generate_transaction(customer_id, holdings)
        producer.send(TOPIC, txn)
        producer.flush()
        print(f'Published event for {customer_id} to {TOPIC}')
        time.sleep(interval_seconds)


if __name__ == '__main__':
    publish_transactions()
