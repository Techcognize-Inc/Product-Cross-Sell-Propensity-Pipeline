from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


import os
import subprocess


def run_weekly_propensity_batch(**kwargs):
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    script = os.path.join(repo_root, 'Batch', 'propensity_signals.py')
    print(f'Running weekly batch job: {script}')
    subprocess.check_call(['python', script])


def run_dbt_propensity_scores(**kwargs):
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    dbt_dir = os.path.join(repo_root, 'dbt')
    print(f'Running dbt in {dbt_dir}')
    subprocess.check_call(['dbt', 'run'], cwd=dbt_dir)


def publish_broadcast_scores(**kwargs):
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    script = os.path.join(repo_root, 'Streaming', 'broadcast_publisher.py')
    print(f'Publishing propensity scores from {script}')
    subprocess.check_call(['python', script])


def refresh_nightly_enrichment(**kwargs):
    print('Refreshing nightly spend enrichment feed')


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
        weekly_batch = PythonOperator(
            task_id='weekly_batch_propensity',
            python_callable=run_weekly_propensity_batch,
        )

        dbt_scores = PythonOperator(
            task_id='dbt_propensity_scores',
            python_callable=run_dbt_propensity_scores,
        )

        publish_scores = PythonOperator(
            task_id='publish_propensity_broadcast',
            python_callable=publish_broadcast_scores,
        )

        nightly_enrichment = PythonOperator(
            task_id='nightly_enrichment_refresh',
            python_callable=refresh_nightly_enrichment,
        )

        weekly_batch >> dbt_scores >> publish_scores

    return dag


dag = build_dag()
