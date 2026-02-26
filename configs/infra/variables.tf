variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "env" {
  type = string
}

variable "snowflake_role" {
  type = string
  default = "ACCOUNTADMIN"
}

variable "snowflake_aws_iam_user_arn" {
  type = string
  default = ""
}

variable "snowflake_external_id" {
  type = string
  default = ""
}
