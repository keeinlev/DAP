# Creating the user, role, warehouse, and grants for DBT to use in EC2

resource "snowflake_warehouse" "transform_dbt" {
  name           = "TRANSFORM_${upper(terraform.workspace)}"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_account_role" "dbt_role" {
  name = "DBT_${upper(terraform.workspace)}_ROLE"
}

# Privilege for ANALYTICS DB usage
resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_grant" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.dbt_role.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.analytics.fully_qualified_name
  }
}

# Privileges for RAW schema
resource "snowflake_grant_privileges_to_account_role" "dbt_raw_schema_grant" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.dbt_role.name
  on_schema {
    schema_name = snowflake_schema.raw.fully_qualified_name
  }
}
resource "snowflake_grant_privileges_to_account_role" "dbt_raw_schema_tables_grant" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.dbt_role.name
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema = snowflake_schema.raw.fully_qualified_name 
    }
  }
}

# Privileges for DBT schema
resource "snowflake_grant_privileges_to_account_role" "dbt_dbt_schema_grant" {
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  account_role_name = snowflake_account_role.dbt_role.name
  on_schema {
    schema_name = snowflake_schema.dbt.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_dbt_schema_existing_tables_grant" {
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  account_role_name = snowflake_account_role.dbt_role.name
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.dbt.fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_dbt_schema_future_tables_grant" {
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  account_role_name = snowflake_account_role.dbt_role.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.dbt.fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_dbt_schema_existing_views_grant" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.dbt_role.name
  on_schema_object {
    all {
      object_type_plural = "VIEWS"
      in_schema          = snowflake_schema.dbt.fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_dbt_schema_future_views_grant" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.dbt_role.name
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = snowflake_schema.dbt.fully_qualified_name
    }
  }
}

# Create DBT user
resource "snowflake_user" "dbt_user" {
  name          = "DBT_${upper(terraform.workspace)}_USER"
  login_name    = "dbt_${terraform.workspace}"
  default_role  = snowflake_account_role.dbt_role.name
  default_warehouse = snowflake_warehouse.transform_dbt.name

  rsa_public_key = file("${path.module}/keys/dbt_${terraform.workspace}_public_key.pub")
}

# Assign role to user
resource "snowflake_grant_account_role" "dbt_user_role" {
  role_name = snowflake_account_role.dbt_role.name
  user_name = snowflake_user.dbt_user.name
}
