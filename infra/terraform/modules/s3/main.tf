variable "project" { type = string }
variable "env"     { type = string }

locals {
  prefix = "${var.project}-${var.env}"
}

resource "aws_s3_bucket" "raw" {
  bucket = "${local.prefix}-raw"
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${local.prefix}-artifacts"
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration { status = "Enabled" }
}

output "raw_bucket_name" { value = aws_s3_bucket.raw.bucket }
output "artifacts_bucket_name" { value = aws_s3_bucket.artifacts.bucket }
