output "database_name" {
  value       = postgresql_database.event_log.name
  description = "Local event-log database used by integration tests."
}

output "database_url" {
  value       = "postgres://${var.app_role}:${var.app_password}@${var.host}:${var.port}/${var.database}?sslmode=disable"
  description = "Connection string for the local event-log database (local credentials only)."
  sensitive   = true
}
