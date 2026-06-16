# PostgreSQL provider configured for a local development server. It connects as the local superuser
# to provision the application role and the event-log database the integration tests use. It is for
# local development only and must never target a managed or production instance.
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

# Event-log database owned by the application role. The Repository owns the tables inside it
# (postgres.Repository.EnsureSchema applies idempotent CREATE TABLE statements), so no table DDL
# lives here — the relational schema must stay in lockstep with the Go types that read and write it.
resource "postgresql_database" "event_log" {
  name  = var.database
  owner = postgresql_role.app.name
}
