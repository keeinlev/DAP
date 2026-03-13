resource "aws_kinesis_stream" "events_stream" {
  name             = "dap-kinesis-stream-${terraform.workspace}"
  shard_count      = 1
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

resource "aws_kinesis_firehose_delivery_stream" "events_firehose_stream" {
  name        = "dap-kinesis-firehose-stream-${terraform.workspace}"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.events.arn
    buffering_size = 128
    buffering_interval = 300

    data_format_conversion_configuration  {
      enabled = true

      input_format_configuration {
        deserializer {
            open_x_json_ser_de {}
          }
      }

      output_format_configuration {
        serializer {
            parquet_ser_de {
              compression = "SNAPPY"
            }
        }
      }

      schema_configuration {
        database_name = aws_glue_catalog_database.events.name
        table_name    = aws_glue_catalog_table.events.name
        role_arn      = aws_iam_role.firehose.arn
      }
    }
  }
}