#!/usr/bin/env bash
set -e

ENV=$(terraform workspace show | awk '{printf $1}')

echo "=== Phase 1: Create RSA keypair for DBT Snowflake user ==="
mkdir -p core/keys
openssl genrsa -out core/keys/dbt_${ENV}_private_key.pem 2048
openssl rsa -in core/keys/dbt_${ENV}_private_key.pem -pubout -out core/keys/dbt_${ENV}_public_key.pub

echo "=== Phase 2: Terraform AWS bootstrap core ==="
dotenv run -- terraform apply -target=module.core -auto-approve -var lambda_ingestion_img_id=""

echo "=== Phase 3: Fetch and Persist Snowflake IAM details ==="

DESC=$(python core/get_snowflake_integration_vars.py $ENV)

SNOWFLAKE_IAM_USER_ARN=$(echo "$DESC" | grep STORAGE_AWS_IAM_USER_ARN | awk '{printf $2}')
SNOWFLAKE_EXTERNAL_ID=$(echo "$DESC" | grep STORAGE_AWS_EXTERNAL_ID | awk '{printf $2}')

aws ssm put-parameter --cli-input-json '{"Name": "/dap/'$ENV'/snowflake_iam_user_arn", "Value": "'$SNOWFLAKE_IAM_USER_ARN'", "Type": "String", "Overwrite": true}' \
  --profile terraform-admin

aws ssm put-parameter --cli-input-json '{"Name": "/dap/'$ENV'/snowflake_external_id", "Value": "'$SNOWFLAKE_EXTERNAL_ID'", "Type": "String", "Overwrite": true}' \
  --profile terraform-admin

echo "=== Phase 4: Build and push the Ingestion Lambda image to ECR ==="

ECR_REPO_URL=$(aws ecr describe-repositories --repository-name ingestion-lambda-$ENV --query repositories[0].repositoryUri --output text --region us-east-2 --profile terraform-admin)
docker build --provenance false --no-cache --platform linux/arm64 ../lambda -t lambda-ingestion
LAMBDA_INGEST_IMG_ID=$(docker images --format "table {{.Repository}}:{{.Tag}}\t{{.ID}}" | grep "lambda-ingestion:latest" | awk '{printf $2}')
docker tag $LAMBDA_INGEST_IMG_ID $ECR_REPO_URL:$LAMBDA_INGEST_IMG_ID
docker push $ECR_REPO_URL:$LAMBDA_INGEST_IMG_ID

echo "=== Phase 5: Terraform finalize ==="
dotenv run -- terraform apply -auto-approve -var lambda_ingestion_img_id=$LAMBDA_INGEST_IMG_ID

echo "Bootstrap complete."
echo "Check the AWS Console > API Gateway > APIs > dap-$ENV-ingestion-api > Stages (sidebar) for the API endpoint Invoke URL, and likewise, see API Keys in the same sidebar for the key to add to the HTTP x-api-key Header for sending events. The endpoint path is /ingest and takes a POST method."
