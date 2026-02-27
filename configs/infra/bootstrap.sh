#!/usr/bin/env bash
set -e

ENV=$1

if [ -z "$ENV" ]; then
  echo "usage: ./bootstrap.sh dev"
  exit 1
fi

echo "=== Phase 1: Create RSA keypair for DBT Snowflake user ==="
mkdir -p keys
openssl genrsa -out keys/dbt_private_key.pem 2048
openssl rsa -in keys/dbt_private_key.pem -pubout -out keys/dbt_public_key.pub

echo "=== Phase 2: Terraform AWS bootstrap ==="
dotenv run -- terraform apply -auto-approve -var="env=$ENV"

echo "=== Phase 3: Fetch and Persist Snowflake IAM details ==="

DESC=$(python core/get_snowflake_integration_vars.py $ENV)

SNOWFLAKE_IAM_USER_ARN=$(echo "$DESC" | grep STORAGE_AWS_IAM_USER_ARN | awk '{print $2}')
SNOWFLAKE_EXTERNAL_ID=$(echo "$DESC" | grep STORAGE_AWS_EXTERNAL_ID | awk '{print $2}')

echo "snowflake_aws_iam_user_arn = \"$SNOWFLAKE_IAM_USER_ARN\"" > core/bootstrap.auto.tfvars
echo "snowflake_external_id = \"$SNOWFLAKE_EXTERNAL_ID\"" >> core/bootstrap.auto.tfvars

echo "=== Phase 4: Terraform finalize ==="
dotenv run -- terraform apply -auto-approve -var="env=$ENV"

echo "Bootstrap complete."