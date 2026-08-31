


resource "google_service_account" "producer" {
  account_id   = "orbitalsense-producer"
  display_name = "OrbitalSense telemetry producer"
}

resource "google_service_account" "beam" {
  account_id   = "orbitalsense-beam"
  display_name = "OrbitalSense Dataflow worker"
}



# Producer
resource "google_pubsub_topic_iam_member" "producer_publish" {
  topic  = google_pubsub_topic.telemetry.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.producer.email}"
}

# Beam subscriber
resource "google_pubsub_subscription_iam_member" "beam_subscriber" {
  subscription = google_pubsub_subscription.beam.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.beam.email}"
}


# BigQuery
resource "google_bigquery_dataset_iam_member" "beam_raw" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.beam.email}"
}

resource "google_bigquery_dataset_iam_member" "beam_curated" {
  dataset_id = google_bigquery_dataset.curated.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.beam.email}"
}

resource "google_bigquery_dataset_iam_member" "beam_quarantine" {
  dataset_id = google_bigquery_dataset.quarantine.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.beam.email}"
}

resource "google_project_iam_member" "beam_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"

  member = "serviceAccount:${google_service_account.beam.email}"
}




resource "google_cloud_run_v2_service_iam_member" "producer_public" {
  name     = google_cloud_run_v2_service.producer.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Dataflow worker
resource "google_project_iam_member" "beam_dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"

  member = "serviceAccount:${google_service_account.beam.email}"
}

# Dataflow temp storage
resource "google_storage_bucket_iam_member" "beam_dataflow_temp" {
  bucket = google_storage_bucket.dataflow_temp.name
  role   = "roles/storage.objectAdmin"

  member = "serviceAccount:${google_service_account.beam.email}"
}