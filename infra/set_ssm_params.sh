#!/usr/bin/env bash
# Just a standalone call for Phase 3 of bootstrap.sh, also just used for testing the bootstrap process.
DESC=$(python core/get_snowflake_integration_vars.py dev)
SNOWFLAKE_IAM_USER_ARN=$(echo "$DESC" | grep STORAGE_AWS_IAM_USER_ARN | awk '{printf $2}')
SNOWFLAKE_EXTERNAL_ID=$(echo "$DESC" | grep STORAGE_AWS_EXTERNAL_ID | awk '{printf $2}')

aws ssm put-parameter --cli-input-json '{"Name": "/dap/dev/snowflake_iam_user_arn", "Value": "'$SNOWFLAKE_IAM_USER_ARN'", "Type": "String", "Overwrite": true}' \
  --profile terraform-admin

aws ssm put-parameter --cli-input-json '{"Name": "/dap/dev/snowflake_external_id", "Value": "'$SNOWFLAKE_EXTERNAL_ID'", "Type": "String", "Overwrite": true}' \
  --profile terraform-admin