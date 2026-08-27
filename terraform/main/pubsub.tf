
# Pub/Sub -5

resource "google_pubsub_topic" "telemetry" {
  name = "orbitalsense-telemetry"
}

resource "google_pubsub_topic" "dead_letter" {
  name = "orbitalsense-telemetry-dead-letter"
}

resource "google_pubsub_subscription" "beam" {
  name = "orbitalsense-beam-sub"
  topic = google_pubsub_topic.telemetry.name

  ack_deadline_seconds = 60

  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription" "dead_letter" {
  name = "orbitalsense-dead-letter-sub"
  topic = google_pubsub_topic.dead_letter.name

  message_retention_duration = "604899s"
}