# AWS provider configured for local emulation with Moto or LocalStack.
provider "aws" {
  access_key                  = "test"
  secret_key                  = "test"
  region                      = "us-west-2"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    dynamodb = var.base_endpoint
    iam      = var.base_endpoint
    sts      = var.base_endpoint
  }
}

terraform {
  backend "local" {}

  required_version = "~> 1.14"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.22.0"
    }
  }
}

resource "aws_dynamodb_table" "event_log" {
  name         = "evt-local-event-log"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "N"
  }

  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

# Heads table: one small row per entity (pk = entity ID) recording its highest event sequence,
# maintained by the heads projector and read for incremental-rebuild change detection. It carries
# no key beyond pk; entityType is a plain attribute filtered during scans, not a key, so it is not
# declared here.
resource "aws_dynamodb_table" "entity_heads" {
  name         = "evt-local-entity-heads"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }
}

resource "aws_dynamodb_table" "entity_views" {
  name         = "evt-local-entity-views"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "entityType"
    type = "S"
  }

  global_secondary_index {
    name            = "entityType-index"
    hash_key        = "entityType"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}
