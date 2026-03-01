resource "aws_secretsmanager_secret" "snowflake_organization" {
  name = "dev/snowflake/SNOWFLAKE_ORGANIZATION"
}
resource "aws_secretsmanager_secret" "snowflake_account_name" {
  name = "dev/snowflake/SNOWFLAKE_ACCOUNT_NAME"
}
resource "aws_secretsmanager_secret" "snowflake_user" {
  name = "dev/snowflake/SNOWFLAKE_USER"
}
resource "aws_secretsmanager_secret" "snowflake_password" {
  name = "dev/snowflake/SNOWFLAKE_PASSWORD"
}

resource "aws_secretsmanager_secret" "snowflake_dbt_user_private_key" {
  name = "dev/snowflake/SNOWFLAKE_DBT_USER_PRIVATE_KEY"
}
