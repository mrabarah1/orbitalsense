
# terraform provider-1
terraform {
  required_version = ">=1.6.0"

  required_providers {
    google = {
        source = "hashicorp/google"
        version = "~> 7.0"
    }
  }

  backend "gcs" {
    bucket = "orbitalsense-platform-tfstate"
    prefix = "orbitalsense"
  }
}

provider "google" {
  project = var.project_id
  region = var.region
}



