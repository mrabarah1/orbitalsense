
# terraform variables-2
variable "project_id" {
  type = string
  description = "orbitalsense-platform"
}

variable "region" {
  type = string
  default = "us-central1"
}

variable "environment" {
  type = string
  default = "dev"
}