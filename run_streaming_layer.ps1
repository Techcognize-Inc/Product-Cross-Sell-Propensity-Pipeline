$ErrorActionPreference = "Stop"

$project = "productcross-sellpropensitypipeline"

Write-Host "[1/6] Starting Kafka + Flink + streaming services..."
docker compose up -d kafka zookeeper flink-jobmanager flink-taskmanager intent_detector broadcast_publisher crm_consumer realtime_offer_writer producer

Write-Host "[2/6] Publishing one-shot propensity score broadcast snapshot..."
docker exec "${project}-broadcast_publisher-1" bash -lc "BROADCAST_RUN_ONCE=true python Streaming/broadcast_publisher.py"

Write-Host "[3/6] Verifying transaction, product-event and broadcast topics have messages..."
docker exec "${project}-kafka-1" kafka-console-consumer --bootstrap-server kafka:9092 --topic txn.stream --from-beginning --timeout-ms 5000 | head -n 5
docker exec "${project}-kafka-1" kafka-console-consumer --bootstrap-server kafka:9092 --topic product.events --from-beginning --timeout-ms 5000 | head -n 5
docker exec "${project}-kafka-1" kafka-console-consumer --bootstrap-server kafka:9092 --topic propensity_scores.broadcast --from-beginning --timeout-ms 5000 | head -n 5

Write-Host "[4/6] Verifying offer topic receives enriched offers..."
docker exec "${project}-kafka-1" kafka-console-consumer --bootstrap-server kafka:9092 --topic crm.realtime_offers --from-beginning --timeout-ms 10000 | head -n 10

Write-Host "[5/6] Verifying offers persisted to PostgreSQL..."
docker exec "${project}-postgres-1" psql -U airflow -d airflow -c "SELECT COUNT(*) AS offer_rows FROM public.realtime_offers; SELECT customer_id, product, final_score, received_at FROM public.realtime_offers ORDER BY id DESC LIMIT 10;"

Write-Host "[6/6] Verifying realtime intent events persisted to PostgreSQL..."
docker exec "${project}-postgres-1" psql -U airflow -d airflow -c "SELECT COUNT(*) AS intent_rows FROM public.realtime_intent_events; SELECT customer_id, product, pattern_name, composite_score, created_at FROM public.realtime_intent_events ORDER BY created_at DESC LIMIT 10;"

Write-Host "Done."
