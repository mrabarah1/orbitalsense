

resource "google_cloud_run_v2_service" "producer" {
  name     = "orbitalsense-producer"
  location = var.region

  template {
    service_account = google_service_account.producer.email

    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/orbitalsense/producer:latest"

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "PUBSUB_TOPIC"
        value = google_pubsub_topic.telemetry.name
      }
    }
  }

  depends_on = [
    google_project_service.services
  ]
}