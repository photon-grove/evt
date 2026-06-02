output "event_log_table_name" {
  value       = aws_dynamodb_table.event_log.name
  description = "Local event-log table used by integration tests."
}

output "entity_views_table_name" {
  value       = aws_dynamodb_table.entity_views.name
  description = "Local entity-views table used by integration tests."
}
