terraform {
  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.21"
    }
  }
}

provider "postgresql" {
  host            = var.pg_host
  port            = var.pg_port
  username        = var.pg_admin_user
  password        = var.pg_admin_password
  database        = var.pg_database
  sslmode         = "disable"
  connect_timeout = 15
}

# Read-only reporting role -- mirrors provisioning a BI/analyst
# service account on a real warehouse (Snowflake, Databricks, etc).
resource "postgresql_role" "reporting" {
  name     = "claims_reporting"
  login    = true
  password = var.reporting_role_password
}

resource "postgresql_grant" "reporting_select_marts" {
  database    = var.pg_database
  role        = postgresql_role.reporting.name
  schema      = "public"
  object_type = "table"
  privileges  = ["SELECT"]
}
