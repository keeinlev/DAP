output "aws_s3_bucket_id" {
  description = "ID of the Events S3 bucket"
  value = aws_s3_bucket.events.id
}

output "aws_s3_bucket_arn" {
  description = "ARN of the Events S3 bucket"
  value = aws_s3_bucket.events.arn
}

output "aws_ecr_repo_arn" {
  description = "ARN of the ECR repo"
  value = aws_ecr_repository.ingestion_lambda.arn
}

output "aws_ecr_repo_url" {
  description = "URL of the ECR repo"
  value = aws_ecr_repository.ingestion_lambda.repository_url
}

output "snowflake_db_name" {
  description = "Snowflake database name"
  value = snowflake_database.analytics.name
}

output "snowflake_raw_schema_name" {
  description = "Snowflake raw schema name"
  value = snowflake_schema.raw.name
}

output "snowflake_raw_events_table_name" {
  description = "Snowflake raw schema events table name"
  value = snowflake_table.events.name
}

output "snowflake_stage_name" {
  description = "Snowflake stage name"
  value = snowflake_stage.events_stage.name
}

output "aws_iam_snowflake_integration_policy" {
  description = "Anchor for ingestion Snowpipe to depend on"
  value = aws_iam_role.snowflake_storage_integration.assume_role_policy
}
