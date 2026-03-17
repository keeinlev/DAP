variable "aws_s3_bucket_id" {}
variable "aws_s3_bucket_arn" {}
variable "aws_ecr_repo_arn" {}
variable "aws_ecr_repo_url" {}
variable "snowflake_db_name" {}
variable "snowflake_raw_schema_name" {}
variable "snowflake_raw_events_table_name" {}
variable "snowflake_stage_name" {}
variable "aws_iam_snowflake_integration_policy" {}

variable "aws_region" {}

variable "snowflake_role" {}

variable "lambda_ingestion_img_id" {}
