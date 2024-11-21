resource "aws_s3_bucket" "grad-project-cde1" {
  bucket = "capstone-tourist"
}


resource "aws_s3_bucket_versioning" "capstone-bucket" {
  bucket = aws_s3_bucket.grad-project-cde1.id
  versioning_configuration {
    status = "Enabled"
  }
}

