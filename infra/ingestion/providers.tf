terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.36"
    }

    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.90"
    }

    null = {
      source  = "hashicorp/null"
      version = "~> 3.2.4"
    }
  }
}

provider "aws" {
  profile = "terraform-admin"
  region  = var.aws_region
}

provider "snowflake" {
  role = var.snowflake_role # use env vars for account_name, organization_name, user, password
}
