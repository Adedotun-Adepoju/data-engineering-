terraform {
    required_version = ">=1.0"
    backend "local" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  //credentials = file("<NAME>.json") use this if you don't want to use the export env variable
}

# Data Lake Bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name = "${local.data_lake_bucket}_${var.project}" # Concatenate DL bucket and project name
  location = var.region

  # Optional, but recommended settings
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30 //days
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = var.BQ_DATASET
  project = var.project
  location = var.region
}