variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "snowflake_role" {
  type = string
  default = "ACCOUNTADMIN"
}

variable "lambda_ingestion_img_id" {
  type = string
}
