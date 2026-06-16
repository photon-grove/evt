variable "host" {
  type        = string
  default     = "localhost"
  description = "Local PostgreSQL host."
}

variable "port" {
  type        = number
  default     = 5432
  description = "Local PostgreSQL port."
}

variable "superuser" {
  type        = string
  default     = "postgres"
  description = "Local PostgreSQL superuser used to provision the role and database."
}

variable "superuser_password" {
  type        = string
  default     = "postgres"
  description = "Local superuser password (local development only)."
}

variable "app_role" {
  type        = string
  default     = "evt"
  description = "Application login role used by integration tests."
}

variable "app_password" {
  type        = string
  default     = "evt"
  description = "Application role password (local development only)."
}

variable "database" {
  type        = string
  default     = "evt_local"
  description = "Event-log database created for integration tests."
}
