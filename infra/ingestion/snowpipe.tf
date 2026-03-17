resource "null_resource" "wait_for_role_propagation" {
  provisioner "local-exec" {
    command = "sleep 10"  # Introduces a 10-second delay because the IAM role for the Snowflake Integration update needs to complete before the Snowpipe creation kicks off.
                            # Even with the depends_on, it starts too soon without a buffer delay.
  }

  depends_on = [
    var.aws_iam_snowflake_integration_policy
  ]
}

resource "snowflake_pipe" "events" {
  # Snowpipe requires the Snowflake Integration IAM role to be updated, but it is not an explicit dependency in the TF graph, so without a depends_on chain leading back to the
  # role policy, the Snowpipe creation will precede the IAM role update, and will throw an access error. An additional time delay is needed to allow the update to propagate before
  # the Snowpipe can be created (see above null_resource).
  depends_on = [ null_resource.wait_for_role_propagation ]
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
