variable "aws_region" {
  type        = string
  default     = "eu-north-1"
  description = "Region to deploy resources"
}

variable "bucket_name" {
  type        = string
  description = "Name of the S3 bucket to create"
}

variable "folder_name" {
  type        = string
  description = "Folder (prefix) name inside the bucket"
}

variable "env" {
  type        = string
  default     = "dev"
}

# terraform init
# terraform plan -var="bucket_name=ingestion-bucket" -var="folder_name=raw"
# terraform apply -auto-approve