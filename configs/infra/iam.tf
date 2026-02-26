data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "snowflake_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type = "AWS"
      identifiers = length(var.snowflake_aws_iam_user_arn) > 0 ? [var.snowflake_aws_iam_user_arn] : ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    dynamic "condition" {
      for_each = length(var.snowflake_external_id) > 0 ? [1] : []

      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [var.snowflake_external_id]
      }
    }
  }
}

resource "aws_iam_role" "snowflake_storage_integration" {
  name               = "${title(var.env)}SnowflakeStorageIntegration"
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust.json
}

resource "aws_iam_role_policy" "snowflake_s3_access" {
  name = "snowflake-${var.env}-s3-access"
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
