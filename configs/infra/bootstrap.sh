#!/usr/bin/env bash
set -e

ENV=$1

if [ -z "$ENV" ]; then
  echo "usage: ./bootstrap.sh dev"
  exit 1
fi

echo "=== Phase 1: Terraform AWS bootstrap ==="
dotenv run -- terraform apply -auto-approve -var="env=$ENV"

echo "=== Fetching Snowflake IAM details ==="

DESC=$(python get_snowflake_integration_vars.py $ENV)

SNOWFLAKE_IAM_USER_ARN=$(echo "$DESC" | grep STORAGE_AWS_IAM_USER_ARN | awk '{print $2}')
SNOWFLAKE_EXTERNAL_ID=$(echo "$DESC" | grep STORAGE_AWS_EXTERNAL_ID | awk '{print $2}')

echo "snowflake_aws_iam_user_arn = \"$SNOWFLAKE_IAM_USER_ARN\"" > bootstrap.auto.tfvars
echo "snowflake_external_id = \"$SNOWFLAKE_EXTERNAL_ID\"" >> bootstrap.auto.tfvars

echo "=== Phase 2: Terraform finalize ==="
dotenv run -- terraform apply -auto-approve -var="env=$ENV"

echo "Bootstrap complete."