resource "snowflake_database" "analytics" {
  name = "ANALYTICS_${upper(var.env)}"
}

resource "snowflake_schema" "raw" {
  name     = "RAW"
  database = snowflake_database.analytics.name
}

resource "snowflake_schema" "dbt" {
  name     = "DBT"
  database = snowflake_database.analytics.name
}

resource "snowflake_table" "events" {
  database = snowflake_database.analytics.name
  schema   = snowflake_schema.raw.name
  name     = "EVENTS"

  column {
    name = "EVENT_NAME"
    type = "STRING"
  }

  column {
    name = "INGESTED_AT"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "DATA"
    type = "VARIANT"
  }
}

resource "snowflake_file_format" "json_format" {
  name     = "JSON_FORMAT"
  database = snowflake_database.analytics.name
  schema   = snowflake_schema.raw.name
  format_type = "JSON"
}

resource "snowflake_storage_integration" "s3" {
  name                      = "S3_${upper(var.env)}_BUCKET_ACCESS"
  storage_provider          = "S3"
  enabled                   = true
  storage_aws_role_arn      = aws_iam_role.snowflake_storage_integration.arn
  storage_allowed_locations = [
    "s3://${aws_s3_bucket.events.bucket}",
    "s3://${aws_s3_bucket.events.bucket}/*"
  ]
}

resource "snowflake_stage" "events_stage" {
  name                = "EVENTS_STAGE"
  database            = snowflake_database.analytics.name
  schema              = snowflake_schema.raw.name
  url                 = "s3://${aws_s3_bucket.events.bucket}"
  storage_integration = snowflake_storage_integration.s3.name
  file_format         = "FORMAT_NAME = ${snowflake_database.analytics.name}.${snowflake_schema.raw.name}.${snowflake_file_format.json_format.name}"
}
