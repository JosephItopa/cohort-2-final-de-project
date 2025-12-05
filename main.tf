terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}


# S3 BUCKET

resource "aws_s3_bucket" "supabase-bucket-2025" {
  bucket = var.bucket_name

  tags = {
    Environment = var.env
    ManagedBy   = "Terraform"
  }
}

# Enable server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "default" {
  bucket = aws_s3_bucket.data_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Enable versioning (recommended for data lakes)
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_bucket.id

  versioning_configuration {
    status = "Disabled"
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "public_block" {
  bucket = aws_s3_bucket.data_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# S3 FOLDER (Prefix)

resource "aws_s3_object" "folder" {
  bucket = aws_s3_bucket.data_bucket.bucket
  key    = "${var.folder_name}/"   # trailing slash is important!
  content = ""                     # empty object to represent folder
}