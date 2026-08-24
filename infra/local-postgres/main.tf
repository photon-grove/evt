# Local-only PostgreSQL provider: connects as superuser to provision the integration-test role and
# event-log database. Never target managed or production instances.
terraform {
  backend "local" {}

  required_version = "~> 1.14"

  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.25"
    }
  }
}

provider "postgresql" {
  host            = var.host
  port            = var.port
  username        = var.superuser
  password        = var.superuser_password
  sslmode         = "disable"
  connect_timeout = 15
}

# Least-privilege application login role used by the integration suite. Local-only credentials.
resource "postgresql_role" "app" {
  name     = var.app_role
  login    = true
  password = var.app_password
}

# Repository-owned EnsureSchema applies table DDL to keep it aligned with Go types; Terraform
# provisions only the event-log database.
resource "postgresql_database" "event_log" {
  name  = var.database
  owner = postgresql_role.app.name
}
