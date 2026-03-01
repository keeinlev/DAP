resource "aws_ssm_parameter" "snowflake_external_id" {
  name  = "/dap/${terraform.workspace}/snowflake_external_id"
  type  = "String"
  value = "null"

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "snowflake_iam_user_arn" {
  name  = "/dap/${terraform.workspace}/snowflake_iam_user_arn"
  type  = "String"
  value = "null"

  lifecycle {
    ignore_changes = [value]
  }
}

data "aws_ssm_parameter" "snowflake_external_id" {
  name = aws_ssm_parameter.snowflake_external_id.name
}

data "aws_ssm_parameter" "snowflake_iam_user_arn" {
  name = aws_ssm_parameter.snowflake_iam_user_arn.name
}
