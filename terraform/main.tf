terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.86.0"
    }
  }
}

resource "confluent_kafka_cluster" "main" {
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  display_name = "Bins"
  region       = "us-east-2"
  basic {}
  environment {
    id = var.environment_id
  }
}

resource "confluent_service_account" "connector" {
  display_name = "bins-connector"
  description  = "Runs the Bins weather HTTP source"
}

resource "confluent_role_binding" "connector_admin" {
  principal   = "User:${confluent_service_account.connector.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

resource "confluent_connector" "weather_connector" {
  environment { id = var.environment_id }
  kafka_cluster { id = confluent_kafka_cluster.main.id }
  config_sensitive = {}
  config_nonsensitive = {
    "name"                = "weather-http-source"
    "connector.class"     = "HttpSource"
    "kafka.auth.mode"     = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.connector.id
    "output.data.format"  = "AVRO"
    "tasks.max"           = "1"
    "url"                 = "https://api.open-meteo.com/v1/forecast?latitude=51.5072&longitude=-0.1276&current=temperature_2m,precipitation,wind_speed_10m"
    "http.request.method" = "GET"
    "http.offset.mode" = "SIMPLE_INCREMENTING"
    "http.initial.offset" = 0
    "auth.type"           = "none"
    "topic.name.pattern"  = "weather_obs"
    "request.interval.ms" = "60000"
  }
}
