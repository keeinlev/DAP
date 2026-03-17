resource "aws_s3_bucket" "events" {
  bucket = "dap-analytics-events-${terraform.workspace}"
}

resource "aws_s3_bucket_versioning" "events_versioning" {
  bucket = aws_s3_bucket.events.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "events_block" {
  bucket = aws_s3_bucket.events.id

  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls  = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_notification" "snowpipe" {
  bucket = aws_s3_bucket.events.id

  queue {
    queue_arn = snowflake_pipe.events[0].notification_channel
    events    = ["s3:ObjectCreated:*"]
  }
}
