#!/bin/bash
set -e

echo "Fetching dbt private key from Secrets Manager..."

aws secretsmanager get-secret-value \
  --secret-id dbt_snowflake_private_key_${ENV} \
  --query SecretString \
  --output text > /opt/airflow/dbt_private_key.pem

chmod 600 /opt/airflow/dbt_private_key.pem

echo "Starting Airflow..."
exec airflow standalone