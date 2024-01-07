provider "google" {
  project     = "homehuntr"
  region      = "us-east1"
  zone        = "us-east1-c"
  credentials = "./terraform.json"
}

resource "google_storage_bucket" "homehuntr-storage" {
 name          = "homehuntr-storage"
 location      = "us-east1"
 storage_class = "STANDARD"

 uniform_bucket_level_access = true
}