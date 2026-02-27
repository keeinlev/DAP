resource "aws_secretsmanager_secret" "snowflake_organization" {
  name = "SNOWFLAKE_ORGANIZATION"
}
resource "aws_secretsmanager_secret" "snowflake_account_name" {
  name = "SNOWFLAKE_ACCOUNT_NAME"
}
resource "aws_secretsmanager_secret" "snowflake_user" {
  name = "SNOWFLAKE_USER"
}
resource "aws_secretsmanager_secret" "snowflake_password" {
  name = "SNOWFLAKE_PASSWORD"
}

resource "aws_secretsmanager_secret" "snowflake_dbt_user_private_key" {
  name = "SNOWFLAKE_DBT_${upper(terraform.workspace)}_USER_PRIVATE_KEY"
}
