#!/usr/bin/env bash
set -e

ENV=$(terraform workspace show | awk '{printf $1}')

echo "=== Phase 1: Create RSA keypair for DBT Snowflake user ==="
mkdir -p core/keys
openssl genrsa -out core/keys/dbt_${ENV}_private_key.pem 2048
openssl rsa -in core/keys/dbt_${ENV}_private_key.pem -pubout -out core/keys/dbt_${ENV}_public_key.pub

echo "=== Phase 2: Terraform AWS bootstrap ==="
dotenv run -- terraform apply -auto-approve

echo "=== Phase 3: Fetch and Persist Snowflake IAM details ==="

DESC=$(python core/get_snowflake_integration_vars.py $ENV)

SNOWFLAKE_IAM_USER_ARN=$(echo "$DESC" | grep STORAGE_AWS_IAM_USER_ARN | awk '{printf $2}')
SNOWFLAKE_EXTERNAL_ID=$(echo "$DESC" | grep STORAGE_AWS_EXTERNAL_ID | awk '{printf $2}')

aws ssm put-parameter --cli-input-json '{"Name": "/dap/'$ENV'/snowflake_iam_user_arn", "Value": "'$SNOWFLAKE_IAM_USER_ARN'", "Type": "String", "Overwrite": true}' \
  --profile terraform-admin

aws ssm put-parameter --cli-input-json '{"Name": "/dap/'$ENV'/snowflake_external_id", "Value": "'$SNOWFLAKE_EXTERNAL_ID'", "Type": "String", "Overwrite": true}' \
  --profile terraform-admin

echo "=== Phase 4: Terraform finalize ==="
dotenv run -- terraform apply -auto-approve

echo "Bootstrap complete."