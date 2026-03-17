module "core" {
  source = "./core"
  aws_region = var.aws_region
  snowflake_role = var.snowflake_role
}

module "ingestion" {
  source = "./ingestion"
  aws_s3_bucket_id = module.core.aws_s3_bucket_id
  aws_s3_bucket_arn = module.core.aws_s3_bucket_arn
  aws_ecr_repo_arn = module.core.aws_ecr_repo_arn
  aws_ecr_repo_url = module.core.aws_ecr_repo_url
  snowflake_db_name = module.core.snowflake_db_name
  snowflake_raw_schema_name = module.core.snowflake_raw_schema_name
  snowflake_raw_events_table_name = module.core.snowflake_raw_events_table_name
  snowflake_stage_name = module.core.snowflake_stage_name
  aws_iam_snowflake_integration_policy = module.core.aws_iam_snowflake_integration_policy
  aws_region = var.aws_region
  snowflake_role = var.snowflake_role
  lambda_ingestion_img_id = var.lambda_ingestion_img_id
}
