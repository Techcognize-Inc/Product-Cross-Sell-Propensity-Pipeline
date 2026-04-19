import json
import os
import random
import time
from collections import deque
from datetime import datetime, timedelta

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
PRODUCT_EVENT_TOPIC = os.getenv('PRODUCT_EVENT_TOPIC', 'product.events')
DATA_DIR = os.getenv('DATA_DIR', '/app/data')
SCENARIO_SHARE = float(os.getenv('SCENARIO_SHARE', '0.8'))
SCENARIO_CUSTOMERS_PER_PRODUCT = int(os.getenv('SCENARIO_CUSTOMERS_PER_PRODUCT', '12'))
SCENARIO_PRODUCT_WEIGHTS = os.getenv(
    'SCENARIO_PRODUCT_WEIGHTS',
    'home_loan=0.15,credit_card=0.60,personal_loan=0.15,fixed_deposit=0.10',
)


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


def iso_ts(offset: timedelta | None = None):
    event_time = datetime.utcnow()
    if offset is not None:
        event_time = event_time + offset
    return event_time.isoformat()


def build_txn(customer_id: str, category: str, amount: float, holdings: dict,
              event_ts: str, txn_type: str | None = None):
    txn_type = txn_type or ('salary' if category == 'salary' else 'debit')
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
        'event_ts': event_ts,
        'merchant_category': category,
        'transaction_type': txn_type,
        'amount': round(amount, 2),
        'product': product,
        'trigger_signal': trigger_signal,
        'has_credit_card': int(holding.get('has_credit_card', 0) or 0),
        'has_fixed_deposit': int(holding.get('has_fixed_deposit', 0) or 0),
    }


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

    return build_txn(
        customer_id=customer_id,
        category=category,
        amount=amount,
        holdings=holdings,
        event_ts=iso_ts(),
        txn_type=txn_type,
    )


def generate_product_event(txn: dict):
    event_type = random.choices(
        ['page_view', 'quote_request'],
        weights=[0.75, 0.25],
        k=1,
    )[0]

    return {
        'customer_id': txn['customer_id'],
        'event_ts': txn['event_ts'],
        'event_type': event_type,
        'product': txn['product'],
        'source': 'api',
    }


def scenario_event_queues(customers: list[str], holdings: dict):
    scenario_customers = customers[: max(4, SCENARIO_CUSTOMERS_PER_PRODUCT * 4)]
    cohorts = {
        'home_loan': scenario_customers[0:SCENARIO_CUSTOMERS_PER_PRODUCT],
        'credit_card': scenario_customers[SCENARIO_CUSTOMERS_PER_PRODUCT:SCENARIO_CUSTOMERS_PER_PRODUCT * 2],
        'personal_loan': scenario_customers[SCENARIO_CUSTOMERS_PER_PRODUCT * 2:SCENARIO_CUSTOMERS_PER_PRODUCT * 3],
        'fixed_deposit': scenario_customers[SCENARIO_CUSTOMERS_PER_PRODUCT * 3:SCENARIO_CUSTOMERS_PER_PRODUCT * 4],
    }
    queues = {customer_id: deque() for customer_id in scenario_customers}
    return cohorts, queues


def refill_scenario_queue(customer_id: str, scenario_name: str, holdings: dict, queues: dict):
    if scenario_name == 'home_loan':
        queues[customer_id].append(
            build_txn(customer_id, 'home_improvement', random.uniform(2100.0, 2500.0), holdings, iso_ts())
        )
        return

    if scenario_name == 'credit_card':
        queues[customer_id].extend([
            build_txn(customer_id, 'retail', random.uniform(80.0, 250.0), holdings, iso_ts(timedelta(minutes=-15))),
            build_txn(customer_id, 'retail', random.uniform(90.0, 300.0), holdings, iso_ts(timedelta(minutes=-7))),
            build_txn(customer_id, 'retail', random.uniform(120.0, 450.0), holdings, iso_ts()),
        ])
        return

    if scenario_name == 'personal_loan':
        queues[customer_id].extend([
            build_txn(customer_id, 'salary', random.uniform(5200.0, 7800.0), holdings, iso_ts(timedelta(days=-2)), txn_type='salary'),
            build_txn(customer_id, random.choice(['travel', 'electronics']), random.uniform(400.0, 1800.0), holdings, iso_ts()),
        ])
        return

    if scenario_name == 'fixed_deposit':
        queues[customer_id].extend([
            build_txn(customer_id, 'salary', random.uniform(5500.0, 8000.0), holdings, iso_ts(timedelta(hours=-73)), txn_type='salary'),
            build_txn(customer_id, 'utilities', random.uniform(50.0, 250.0), holdings, iso_ts()),
        ])


def choose_scenario_customer(cohorts: dict, queues: dict, holdings: dict):
    scenario_names = list(cohorts.keys())
    weight_lookup = {name: 1.0 for name in scenario_names}
    for item in SCENARIO_PRODUCT_WEIGHTS.split(','):
        try:
            key, raw_weight = item.split('=')
            key = key.strip()
            if key in weight_lookup:
                weight_lookup[key] = max(0.0, float(raw_weight.strip()))
        except Exception:
            continue

    weights = [weight_lookup[name] for name in scenario_names]
    if sum(weights) == 0.0:
        weights = [1.0 for _ in scenario_names]

    scenario_name = random.choices(scenario_names, weights=weights, k=1)[0]
    customer_id = random.choice(cohorts[scenario_name])
    if not queues[customer_id]:
        refill_scenario_queue(customer_id, scenario_name, holdings, queues)
    return scenario_name, customer_id, queues[customer_id].popleft()


def publish_transactions(interval_seconds=1):
    producer = create_producer()
    customers = load_customers()
    holdings = load_holdings()
    cohorts, queues = scenario_event_queues(customers, holdings)
    random_customers = customers[len(queues):] or customers

    while True:
        if random.random() <= SCENARIO_SHARE:
            scenario_name, customer_id, txn = choose_scenario_customer(cohorts, queues, holdings)
        else:
            scenario_name = 'background'
            customer_id = random.choice(random_customers)
            txn = generate_transaction(customer_id, holdings)
        product_event = generate_product_event(txn)
        producer.send(TOPIC, txn)
        producer.send(PRODUCT_EVENT_TOPIC, product_event)
        producer.flush()
        print(
            f'Published {scenario_name} txn for {customer_id} to {TOPIC} and '
            f'product event to {PRODUCT_EVENT_TOPIC}'
        )
        time.sleep(interval_seconds)


if __name__ == '__main__':
    publish_transactions()
