#!/usr/bin/env bash
# This is what I use while testing the TF bootstrap, since destroying Secrets via TF puts them on schedule for deletion in 7 days, and this is the only way to override that.
# Otherwise, the Secrets would still "exist" and not allow a fresh deploy to create the resources since there are still Secrets at the same paths.
aws secretsmanager delete-secret  --force-delete-without-recovery --profile terraform-admin --secret-id dev/snowflake/SNOWFLAKE_USER
aws secretsmanager delete-secret  --force-delete-without-recovery --profile terraform-admin --secret-id dev/snowflake/SNOWFLAKE_PASSWORD
aws secretsmanager delete-secret  --force-delete-without-recovery --profile terraform-admin --secret-id dev/snowflake/SNOWFLAKE_ACCOUNT_NAME
aws secretsmanager delete-secret  --force-delete-without-recovery --profile terraform-admin --secret-id dev/snowflake/SNOWFLAKE_ORGANIZATION
aws secretsmanager delete-secret  --force-delete-without-recovery --profile terraform-admin --secret-id dev/snowflake/SNOWFLAKE_DBT_USER_PRIVATE_KEY