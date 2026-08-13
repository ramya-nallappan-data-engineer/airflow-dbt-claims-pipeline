variable "pg_host" {
  type    = string
  default = "localhost"
}

variable "pg_port" {
  type    = number
  default = 5432
}

variable "pg_admin_user" {
  description = "Admin user Terraform connects as (must have CREATEROLE)"
  type        = string
  default     = "airflow"
}

variable "pg_admin_password" {
  type      = string
  sensitive = true
}

variable "pg_database" {
  type    = string
  default = "claims_warehouse"
}

variable "reporting_role_password" {
  description = "Password for the new read-only reporting role"
  type        = string
  sensitive   = true
}
