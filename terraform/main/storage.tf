

resource "google_storage_bucket" "terraform_state" {
  name     = "${var.project_id}-tfstate"
  location = var.region

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      num_newer_versions = 5
    }

    action {
      type = "Delete"
    }
  }

  labels = {
    layer       = "terraform-state"
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