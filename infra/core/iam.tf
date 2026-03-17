# For Snowflake integration

locals {
  external_id = data.aws_ssm_parameter.snowflake_external_id.value
  iam_user_arn = data.aws_ssm_parameter.snowflake_iam_user_arn.value

  is_bootstrapped = (local.external_id != "null" && local.iam_user_arn != "null") 
}

data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "snowflake_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type = "AWS"
      identifiers = local.is_bootstrapped ? [local.iam_user_arn] : ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    dynamic "condition" {
      for_each = local.is_bootstrapped ? [1] : []

      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [local.external_id]
      }
    }
  }
}

resource "aws_iam_role" "snowflake_storage_integration" {
  name               = "${title(terraform.workspace)}SnowflakeStorageIntegration"
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust.json
}

resource "aws_iam_role_policy" "snowflake_s3_access" {
  name = "snowflake-${terraform.workspace}-s3-access"
  role = aws_iam_role.snowflake_storage_integration.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.events.arn,
          "${aws_s3_bucket.events.arn}/*"
        ]
      }
    ]
  })
}

# For Snowpipe

# resource "aws_sqs_queue_policy" "snowpipe" {
#   queue_url = data.aws_sqs_queue.snowpipe.url

#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [{
#       Sid = "AllowS3ToSendMessages"
#       Effect = "Allow"
#       Principal = {
#         Service = "s3.amazonaws.com"
#       }
#       Action = "sqs:SendMessage"
#       Resource = snowflake_pipe.events[0].notification_channel
#       Condition = {
#         ArnEquals = {
#           "aws:SourceArn" = aws_s3_bucket.events.arn
#         }
#       }
#     }]
#   })
# }

# For Firehose access

resource "aws_iam_role" "firehose" {
  name = "dap-${terraform.workspace}-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "firehose.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  role = aws_iam_role.firehose.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.events_stream.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject"
        ]
        Resource = [
          aws_s3_bucket.events.arn,
          "${aws_s3_bucket.events.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTableVersion",
          "glue:GetTableVersions"
        ]
        Resource = "*"
      }
    ]
  })
}

# For Lambda ECR and Kinesis access

resource "aws_iam_role" "lambda_exec_role" {
  name = "dap-${terraform.workspace}-lambda-exec-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_ecr" {
  role = aws_iam_role.lambda_exec_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:SetRepositoryPolicy",
          "ecr:GetRepositoryPolicy",
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer"
        ]
        Resource = aws_ecr_repository.ingestion_lambda.arn
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:PutRecords",
          "kinesis:DescribeStream"
        ]
        Resource = aws_kinesis_stream.events_stream.arn
      }
    ]
  })
}

# API Gateway Lambda execution
resource "aws_iam_role" "lambda_execution_role" {
  name = "ingestion-${terraform.workspace}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role = aws_iam_role.lambda_execution_role.id
}

resource "aws_lambda_permission" "apigw_lambda" {
  statement_id = "AllowExecutionFromAPIGateway"
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion[0].function_name
  principal = "apigateway.amazonaws.com"

  source_arn = "${aws_api_gateway_rest_api.ingestion_api.execution_arn}/*/*/*"
}
