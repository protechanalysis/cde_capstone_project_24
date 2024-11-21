terraform {
  backend "s3" {
    bucket = "capstone-tourist"
    key    = "backend/terraform/state"
    region = "eu-west-2"
  }
}
