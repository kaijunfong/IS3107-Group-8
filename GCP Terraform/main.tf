##############################################
# 1. Enable Required Services (APIs)
##############################################
resource "google_project_service" "composer" {
  project = var.project_id
  service = "composer.googleapis.com"
}

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
# 3. Provision a Cloud Composer Environment
##############################################
resource "google_composer_environment" "test" {
  name   = "rental-composer-env"
  region = var.region
 config {
    software_config {
      image_version = "composer-3-airflow-2"
    }
  }
}