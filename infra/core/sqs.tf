resource "aws_sqs_queue" "snowpipe" {
  name = "dap-snowpipe-events-${terraform.workspace}"

  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400
}
