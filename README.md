# Product Cross-Sell Propensity Pipeline

This repository implements a hybrid batch + streaming propensity scoring pipeline for retail banking product cross-sell.

## Architecture

- `Batch/`: PySpark weekly signal engineering job that computes customer-product propensity signals and writes scoring inputs.
- `dbt/`: Data transformation layer with a `mart_propensity_scores` model and configurable score weights.
- `Flinkjobs/`: Windows-native Python/Kafka intent detector that consumes transaction events and broadcast propensity scores, then emits CRM offers.
- `Flinkjobs/churn_trigger_detector.py`: Churn trigger detector consuming `customer.events` and `churn.rules.broadcast`, emitting retention alerts.
- `Airflow/`: DAG to orchestrate weekly batch refresh, broadcast score publication, and nightly enrichment updates.
- `Streaming/`: Kafka producer/consumer helpers for transaction and product event streams.
- `Streamlit/`: Marketing dashboard for live intent feed, campaign builder, and score overview.

## Docker Compose

This repo includes a root `docker-compose.yml` that starts:
- Zookeeper
- Kafka
- PostgreSQL
- Airflow webserver
- Streamlit dashboard
- Kafka transaction producer
- Kafka propensity score broadcast publisher
- Churn rule broadcaster and churn trigger detector
- CRM Kafka consumer
- Python/Kafka intent detector service

To start the stack:

```bash
docker compose up --build
```

To start only the producer and Streamlit app:

```bash
docker compose up --build producer streamlit
```

## Sample data

The repository now includes `data/generate_sample_data.py` to build realistic sample data for batch and streaming layers using the **UCI Bank Marketing Dataset** (archive.ics.uci.edu/ml/datasets/Bank+Marketing), scaled to **500K customers** as per requirements.

- **Demographics**: Age, income band (mocked), region (mocked) from UCI
- **Product holdings**: Home loan (housing), personal loan (loan) from UCI; credit card, fixed deposit mocked
- **Campaign history**: Last response, campaign touches from UCI campaign data
- **Transaction history**: Mocked using Faker, linked to UCI customers

Run it before batch execution:

```bash
python data/generate_sample_data.py
```

The PySpark job in `Batch/propensity_signals.py` now reads from `data/*.parquet` and computes the transaction history, demographics, product holdings, and campaign signals required for propensity scoring.

The streaming producer in `Streaming/producer.py` now loads generated customer IDs and holdings from the same sample dataset so intents are emitted against realistic customer profiles.

## Key concepts

- Batch compute 12 product propensity signals per customer.
- Persist scores to PostgreSQL and publish them as a broadcast stream to a Python/Kafka intent detector.
- Real-time transaction stream feeds the Windows-native intent detector for home loan, credit card, FD, and personal loan intent.
- Real-time customer event stream feeds churn trigger detection with 7-day PRODUCT_CLOSURE -> SALARY_REDIRECT pattern alerts.
- Retention alerts are persisted to `public.retention_alerts` and enriched in Airflow with RM assignment and contact windows.
- Streamlit dashboard exposes live offers and campaign filters.

## Getting started

1. Install dependencies for each layer.
2. Configure Kafka, PostgreSQL, and Airflow connections.
3. Run `Batch/propensity_signals.py` for weekly score preparation.
4. Run `python Flinkjobs/intent_detector.py` to start the Windows-native intent detector.
5. Launch `Streamlit/app.py` to view marketing operations.

## Jenkins CI/CD

This repository now includes a root `Jenkinsfile` with CI and optional CD stages:

- Checkout source
- Install Python dependencies
- Validate key pipeline modules (`py_compile`)
- Run focused unit tests (`tests/test_pipeline.py`)
- Build deployment images with Docker Compose
- Optional deployment (local node or remote SSH target)

### Parameters

- `RUN_DEPLOY`: if `true`, executes deployment stage
- `BUILD_DEPLOYMENT_IMAGES`: if `true`, builds deployment images during CI; leave `false` for faster local validation
- `DEPLOY_HOST`: SSH target in the form `user@host`; leave empty for local deploy
- `DEPLOY_PATH`: repository path on deployment target
- `DEPLOY_BRANCH`: branch to deploy
- `COMPOSE_FILE`: compose file path (default `docker-compose.yml`)
- `DEPLOY_STACK`: space-separated compose services to roll out

### Required Jenkins credentials for remote deploy

- `deploy-ssh-key` (SSH Username with private key)

### Helper script

- `ci/scripts/deploy_compose.sh` handles local/remote rollout with `docker compose up -d --build`.

### Jenkins in Docker Compose

Jenkins is included in `docker-compose.yml` and exposed on port `8082` (to avoid conflicts with Airflow `8080` and Flink `8081`).

Start Jenkins:

```bash
docker compose up -d jenkins
```

Access Jenkins UI:

```text
http://localhost:8082
```

Get initial admin password:

```bash
docker compose exec -T jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```
