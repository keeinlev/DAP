resource "snowflake_pipe" "events" {
  name     = "DAP_EVENTS_PIPE"
  database = var.snowflake_db_name
  schema   = var.snowflake_raw_schema_name

  auto_ingest = true

  copy_statement = <<SQL
COPY INTO ${var.snowflake_db_name}.${var.snowflake_raw_schema_name}.${var.snowflake_raw_events_table_name} (event_id, event_type, event_version, ingested_at, payload)
FROM (
  SELECT
    $1:event_id::string,
    $1:event_type::string,
    $1:event_version::int,
    $1:ingested_at::timestamp_ntz(9),
    PARSE_JSON($1:payload)
    FROM @${var.snowflake_db_name}.${var.snowflake_raw_schema_name}.${var.snowflake_stage_name}
)
FILE_FORMAT = (TYPE = PARQUET)
SQL
}

resource "aws_s3_bucket_notification" "snowpipe" {
  bucket = var.aws_s3_bucket_id

  queue {
    queue_arn = snowflake_pipe.events.notification_channel
    events    = ["s3:ObjectCreated:*"]
  }
}
