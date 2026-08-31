
# terraform variables-2
variable "project_id" {
  type = string
  description = "orbitalsense-platform"
}

variable "region" {
  type = string
  description = "Google Cloud region"
  default = "us-central1"
}

variable "environment" {
  type = string
  description = "Development environment"
  default = "dev"
}


variable "pipeline_version" {
  type        = string
  description = "Beam pipeline version"
  default     = "1.0.0"
}

variable "producer_image_tag" {
  type        = string
  description = "Producer container image tag"
  default     = "1.0.0"
}

variable "beam_worker_service_account" {
  type        = string
  description = "Service account used by the Dataflow worker"
  default     = "orbitalsense-beam"
}