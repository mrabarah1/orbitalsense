
# Service Accounts- 7

resource "google_service_account" "producer" {
  account_id = "orbitalsense-producer"
  display_name = "OrbitalSense telemetry producer"
}

resource "google_service_account" "beam" {
  account_id = "orbitalsense-beam"
  display_name = "OrbitalSense Beam pipeline"
}

# Producer
resource "google_pubsub_topic_iam_member" "producer_publish" {
  topic = google_pubsub_topic.telemetry.name
  role = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.producer.email}"
}

# Beam
resource "google_pubsub_iam_member" "beam_dataflow_worker" {
  project = var.project_id
  role = "roles/dataflow.worker"
  member = "serviceAccount:${google_service_account.beam.email}"
}


# BigQuery Access
resource "google_project_iam_member" "beam_bigquery" {
  project = var.project_id
  role = "roles/bigquery.dataEditor"
  member = "serviceAccount:${google_service_account.beam.email}"
}

resource "google_project_iam_member" "beam_bigquery_job" {
  project = var.project_id
  role = "roles/bigquery.jobUser"
  member = "serviceAccount:${google_service_account.beam.email}"
}

# Artificial Registry
resource "google_artifact_registry_repository" "containers" {
  location = var.region
  repository_id = "orbitalsense"
  description = "orbitalsense container images"
  format = "DOCKER"
}