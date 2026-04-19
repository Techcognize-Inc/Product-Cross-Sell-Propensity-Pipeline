$ErrorActionPreference = "Stop"

# Container names
$sparkContainer = "spark-job"
$appContainer = "productcross-sellpropensitypipeline-streamlit-1"
$postgresContainer = "productcross-sellpropensitypipeline-postgres-1"

Write-Host "[1/6] Checking Docker is available..."
docker ps | Out-Null

Write-Host "[2/6] Ensuring PostgreSQL JDBC jar exists in Spark container..."
docker exec $sparkContainer bash -lc "test -f /opt/spark/jars/postgresql-42.7.3.jar || curl -sSL -o /opt/spark/jars/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"

Write-Host "[3/6] Running Spark signals job..."
docker exec $sparkContainer /opt/spark/bin/spark-submit --jars /opt/spark/jars/postgresql-42.7.3.jar /app/propensity_signals.py

Write-Host "[4/6] Running dbt seed..."
docker exec $appContainer dbt seed --project-dir /app/dbt --profiles-dir /app/dbt --full-refresh

Write-Host "[5/6] Running dbt models..."
docker exec $appContainer dbt run --project-dir /app/dbt --profiles-dir /app/dbt

Write-Host "[6/6] Verifying output tables and score stats..."
docker exec $postgresContainer psql -U airflow -d airflow -c "SELECT (SELECT COUNT(*) FROM raw.propensity_inputs) AS raw_rows, (SELECT COUNT(*) FROM public.propensity_inputs) AS dbt_input_rows, (SELECT COUNT(*) FROM public.mart_propensity_scores) AS score_rows, (SELECT COUNT(*) FROM public.mart_campaign_lists) AS campaign_rows; SELECT ROUND(MIN(propensity_score)::numeric,2) AS min_score, ROUND(AVG(propensity_score)::numeric,2) AS avg_score, ROUND(MAX(propensity_score)::numeric,2) AS max_score FROM public.mart_propensity_scores;"

Write-Host "Pipeline run complete."
