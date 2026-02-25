resource "aws_s3_bucket" "events" {
  bucket = "dap-analytics-events-${var.env}"
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