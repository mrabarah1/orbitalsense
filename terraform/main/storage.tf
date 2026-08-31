resource "google_storage_bucket" "raw" {
  name     = "${var.project_id}-orbitalsense-raw"
  location = var.region

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }

    action {
      type = "Delete"
    }
  }

  labels = {
    layer       = "raw"
    environment = var.environment
  }

  depends_on = [
    google_project_service.services
  ]
}

resource "google_storage_bucket" "dataflow_temp" {
  name     = "${var.project_id}-dataflow-temp"
  location = var.region

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7
    }

    action {
      type = "Delete"
    }
  }

  labels = {
    purpose     = "dataflow-temp"
    environment = var.environment
  }

  depends_on = [
    google_project_service.services
  ]
}