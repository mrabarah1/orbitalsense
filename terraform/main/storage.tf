
# Raw GCS bucket-4
resource "google_storage_bucket" "raw" {
  name = "${var.project_id}-orbitalsense-raw"
  location = var.region

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }

    action {
      type = "Delete"
    }
  }
}

# What is the retention policy for raw data?

# Raw landing data is retained for 30 days to support replay,
# Investigation and incident recovery while controlling storage cost.