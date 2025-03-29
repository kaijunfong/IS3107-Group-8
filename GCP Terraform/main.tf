##############################################
# 1. Enable Required Services (APIs)
##############################################
resource "google_project_service" "bigquery" {
  project = var.project_id
  service = "bigquery.googleapis.com"
}

resource "google_project_service" "cloudbuild" {
  project = var.project_id
  service = "cloudbuild.googleapis.com"
}

resource "google_project_service" "vertex_ai" {
  project = var.project_id
  service = "aiplatform.googleapis.com"
}

resource "google_project_service" "compute" {
  project = var.project_id
  service = "compute.googleapis.com"
}

resource "google_project_service" "iam" {
  project = var.project_id
  service = "iam.googleapis.com"
}

resource "google_project_service" "storage" {
  project = var.project_id
  service = "storage.googleapis.com"
}

##############################################
# 2. Create a BigQuery Dataset
##############################################
resource "google_bigquery_dataset" "rental_dataset" {
  dataset_id                  = "rental_dataset"
  project                     = var.project_id
  location                    = var.region
  description                 = "Dataset for storing rental data and features"
  delete_contents_on_destroy  = true
}

##############################################
# 3. Create a GCS Bucket for GeoJSON
##############################################

resource "google_storage_bucket" "geojson_bucket" {
  name          = var.dag_bucket_name
  location      = var.region
  force_destroy = true
}
