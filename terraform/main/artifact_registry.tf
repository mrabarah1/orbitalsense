

resource "google_artifact_registry_repository" "containers" {
  location      = var.region
  repository_id = "orbitalsense"
  description   = "OrbitalSense container images"
  format        = "DOCKER"

  depends_on = [
    google_project_service.services
  ]
}