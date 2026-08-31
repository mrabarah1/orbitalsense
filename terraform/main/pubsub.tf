


resource "google_pubsub_topic" "telemetry" {
  name = "orbitalsense-telemetry"

  depends_on = [
    google_project_service.services
  ]
}

resource "google_pubsub_topic" "dead_letter" {
  name = "orbitalsense-telemetry-dead-letter"

  depends_on = [
    google_project_service.services
  ]
}

resource "google_pubsub_subscription" "beam" {
  name  = "orbitalsense-beam-sub"
  topic = google_pubsub_topic.telemetry.id

  ack_deadline_seconds = 60

  message_retention_duration = "604800s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "60s"
  }

  depends_on = [
    google_project_service.services
  ]
}

resource "google_pubsub_subscription" "dead_letter" {
  name  = "orbitalsense-dead-letter-sub"
  topic = google_pubsub_topic.dead_letter.id

  message_retention_duration = "604800s"

  depends_on = [
    google_project_service.services
  ]
}