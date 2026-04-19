from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


import os
import subprocess
import time
import urllib.request

import psycopg2


DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'user': os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASS', 'airflow'),
    'dbname': os.getenv('DB_NAME', 'airflow'),
}

PROJECT_ROOT = os.getenv('PROJECT_ROOT', '/opt/airflow/project')
DATA_ROOT = os.getenv('DATA_ROOT', '/data')
STREAMLIT_HEALTH_URLS = [
    u.strip() for u in os.getenv(
        'STREAMLIT_HEALTH_URLS',
        'http://streamlit:8501/healthz,http://localhost:8501/healthz',
    ).split(',') if u.strip()
]

REQUIRED_BATCH_FILES = [
    'transactions.parquet',
    'product_holdings.parquet',
    'demographics.parquet',
    'campaign_history.parquet',
]

SIGNAL_COLS_12 = [
    'total_spend_30d',
    'txn_count_30d',
    'avg_txn_30d',
    'unique_category_count_30d',
    'home_improvement_spend_30d',
    'electronics_spend_30d',
    'travel_spend_30d',
    'salary_credit_30d',
    'debit_txn_count_30d',
    'days_since_last_txn',
    'existing_products_flag',
    'campaign_response_flag',
]


def run_weekly_propensity_batch(**kwargs):
    script = os.path.join(PROJECT_ROOT, 'Batch', 'propensity_signals.py')
    print(f'Running weekly batch job: {script}')
    subprocess.check_call(['python', script])


def run_dbt_propensity_scores(**kwargs):
    dbt_dir = os.path.join(PROJECT_ROOT, 'dbt')
    print(f'Running dbt propensity score model in {dbt_dir}')
    dbt_env = os.environ.copy()
    dbt_env['DBT_TARGET_PATH'] = '/tmp/dbt_target'
    dbt_env['DBT_PACKAGES_INSTALL_PATH'] = '/tmp/dbt_packages'
    dbt_env['DBT_PROFILES_DIR'] = dbt_dir
    subprocess.check_call(
        ['dbt', 'run', '--no-partial-parse', '--select', 'mart_propensity_scores'],
        cwd=dbt_dir,
        env=dbt_env,
    )


def run_dbt_campaign_lists(**kwargs):
    dbt_dir = os.path.join(PROJECT_ROOT, 'dbt')
    print(f'Running dbt campaign list model in {dbt_dir}')
    dbt_env = os.environ.copy()
    dbt_env['DBT_TARGET_PATH'] = '/tmp/dbt_target'
    dbt_env['DBT_PACKAGES_INSTALL_PATH'] = '/tmp/dbt_packages'
    dbt_env['DBT_PROFILES_DIR'] = dbt_dir
    subprocess.check_call(
        ['dbt', 'run', '--no-partial-parse', '--select', 'mart_campaign_lists'],
        cwd=dbt_dir,
        env=dbt_env,
    )


def publish_broadcast_scores(**kwargs):
    script = os.path.join(PROJECT_ROOT, 'Streaming', 'broadcast_publisher.py')
    print(f'Publishing propensity scores from {script}')
    broadcast_env = os.environ.copy()
    broadcast_env['KAFKA_BOOTSTRAP_SERVERS'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    broadcast_env['BROADCAST_TOPIC'] = os.getenv('BROADCAST_TOPIC', 'propensity_scores.broadcast')
    broadcast_env['BROADCAST_RUN_ONCE'] = 'true'
    broadcast_env['DB_HOST'] = os.getenv('DB_HOST', 'postgres')
    broadcast_env['DB_USER'] = os.getenv('DB_USER', 'airflow')
    broadcast_env['DB_PASS'] = os.getenv('DB_PASS', 'airflow')
    broadcast_env['DB_NAME'] = os.getenv('DB_NAME', 'airflow')
    subprocess.check_call(['python', script], env=broadcast_env)


def refresh_nightly_enrichment(**kwargs):
    """Materialize latest 7-day category spend snapshot for downstream analytics."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics.nightly_spend_enrichment (
                    as_of_date         DATE NOT NULL,
                    customer_id        VARCHAR(255) NOT NULL,
                    merchant_category  VARCHAR(128) NOT NULL,
                    total_spend        DOUBLE PRECISION NOT NULL,
                    event_count        INTEGER NOT NULL,
                    source_updated_at  TIMESTAMPTZ,
                    refreshed_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (as_of_date, customer_id, merchant_category)
                )
                """
            )
            cur.execute(
                """
                WITH latest AS (
                    SELECT
                        customer_id,
                        merchant_category,
                        total_spend,
                        event_count,
                        updated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY customer_id, merchant_category
                            ORDER BY window_end_ms DESC
                        ) AS rn
                    FROM public.propensity_enrichment_feed
                )
                INSERT INTO analytics.nightly_spend_enrichment (
                    as_of_date,
                    customer_id,
                    merchant_category,
                    total_spend,
                    event_count,
                    source_updated_at,
                    refreshed_at
                )
                SELECT
                    CURRENT_DATE,
                    customer_id,
                    merchant_category,
                    total_spend,
                    event_count,
                    updated_at,
                    CURRENT_TIMESTAMP
                FROM latest
                WHERE rn = 1
                ON CONFLICT (as_of_date, customer_id, merchant_category)
                DO UPDATE SET
                    total_spend = EXCLUDED.total_spend,
                    event_count = EXCLUDED.event_count,
                    source_updated_at = EXCLUDED.source_updated_at,
                    refreshed_at = CURRENT_TIMESTAMP
                """
            )
        conn.commit()
        print('Nightly enrichment refresh completed successfully.')
    finally:
        conn.close()


def validate_batch_inputs(**kwargs):
    """Fail fast if expected batch parquet inputs are missing or empty."""
    missing = []
    empty = []

    for name in REQUIRED_BATCH_FILES:
        fp = os.path.join(DATA_ROOT, name)
        if not os.path.exists(fp):
            missing.append(fp)
            continue
        if os.path.getsize(fp) == 0:
            empty.append(fp)

    if missing or empty:
        msg_parts = []
        if missing:
            msg_parts.append(f"missing files: {missing}")
        if empty:
            msg_parts.append(f"empty files: {empty}")
        raise AirflowException(
            'Batch input validation failed - ' + '; '.join(msg_parts)
        )

    print(f'Batch input validation passed for {len(REQUIRED_BATCH_FILES)} files in {DATA_ROOT}.')


def validate_signal_job_output(**kwargs):
    """Validation for item 30: ensure signal batch wrote 12 columns for 500K+ rows."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM raw.propensity_inputs')
            row_count = cur.fetchone()[0]
            if row_count < 500000:
                raise AirflowException(
                    f'Item 30 validation failed: raw.propensity_inputs has {row_count:,} rows; expected at least 500,000.'
                )

            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'raw'
                  AND table_name = 'propensity_inputs'
                """
            )
            present_cols = {r[0] for r in cur.fetchall()}
            missing_cols = [c for c in SIGNAL_COLS_12 if c not in present_cols]
            if missing_cols:
                raise AirflowException(
                    f'Item 30 validation failed: missing signal columns in raw.propensity_inputs: {missing_cols}'
                )

        print(f'Item 30 validation passed: {row_count:,} rows with all 12 signal columns present.')
    finally:
        conn.close()


def validate_streamlit_feed_contract(**kwargs):
    """Validation for item 35: Streamlit is healthy and feed query columns are available."""
    last_error = None
    for attempt in range(1, 11):
        for health_url in STREAMLIT_HEALTH_URLS:
            try:
                with urllib.request.urlopen(health_url, timeout=10) as resp:
                    if resp.getcode() == 200:
                        last_error = None
                        break
                    last_error = f'{health_url}: HTTP {resp.getcode()}'
            except Exception as exc:
                last_error = f'{health_url}: {exc}'
        if last_error is None:
            break
        if attempt < 10:
            time.sleep(min(3 * attempt, 15))

    if last_error:
        raise AirflowException(f'Item 35 validation failed: Streamlit health check did not return 200. Last error: {last_error}')

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ro.received_at,
                    ro.customer_id,
                    COALESCE(
                        mcl.campaign_segment,
                        CASE
                            WHEN ro.final_score >= 90 THEN 'HOT'
                            WHEN ro.final_score >= 75 THEN 'WARM'
                            ELSE 'WATCH'
                        END
                    ) AS customer_segment,
                    ro.product,
                    ro.final_score AS composite_intent_score,
                    ro.trigger_event
                FROM public.realtime_offers ro
                LEFT JOIN public.mart_campaign_lists mcl
                  ON mcl.customer_id = ro.customer_id
                 AND mcl.product_family = ro.product
                ORDER BY ro.received_at DESC
                LIMIT 1
                """
            )
            _ = cur.fetchone()

        print('Item 35 validation passed: Streamlit health endpoint is up and feed SQL contract is valid.')
    finally:
        conn.close()


def build_dag():
    default_args = {
        'owner': 'data-engineering',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
    }

    with DAG(
        dag_id='propensity_scoring_pipeline',
        start_date=datetime(2025, 1, 1),
        schedule_interval='0 3 * * 1',
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
    ) as dag:
        validate_batch = PythonOperator(
            task_id='validate_batch_inputs',
            python_callable=validate_batch_inputs,
            execution_timeout=timedelta(minutes=5),
            retries=0,
        )

        weekly_batch = PythonOperator(
            task_id='weekly_batch_propensity',
            python_callable=run_weekly_propensity_batch,
            execution_timeout=timedelta(minutes=90),
        )

        dbt_scores = PythonOperator(
            task_id='dbt_propensity_scores',
            python_callable=run_dbt_propensity_scores,
            execution_timeout=timedelta(minutes=30),
        )

        dbt_campaign_lists = PythonOperator(
            task_id='dbt_campaign_lists',
            python_callable=run_dbt_campaign_lists,
            execution_timeout=timedelta(minutes=30),
        )

        publish_scores = PythonOperator(
            task_id='publish_propensity_broadcast',
            python_callable=publish_broadcast_scores,
            execution_timeout=timedelta(minutes=45),
        )

        validate_item_30 = PythonOperator(
            task_id='validate_item_30_signals',
            python_callable=validate_signal_job_output,
            execution_timeout=timedelta(minutes=5),
            retries=0,
        )

        validate_item_35 = PythonOperator(
            task_id='validate_item_35_streamlit_feed',
            python_callable=validate_streamlit_feed_contract,
            execution_timeout=timedelta(minutes=5),
            retries=0,
        )

        nightly_enrichment = PythonOperator(
            task_id='nightly_enrichment_refresh',
            python_callable=refresh_nightly_enrichment,
            execution_timeout=timedelta(minutes=5),
        )

        validate_batch >> weekly_batch >> validate_item_30 >> dbt_scores >> dbt_campaign_lists >> publish_scores >> validate_item_35

    return dag


dag = build_dag()
