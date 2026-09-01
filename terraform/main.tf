terraform {
  required_version = ">= 1.5"
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

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-south-1" # Mumbai -- closest region to Bangalore
}

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for the lakehouse"
  type        = string
  default     = "kaushik-realtime-lakehouse"
}

variable "environment" {
  type    = string
  default = "dev"
}

# --- S3 bucket that backs Bronze/Silver/Gold Delta tables + streaming checkpoints ---
resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.bucket_name}-${var.environment}"

  tags = {
    Project     = "realtime-lakehouse"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled" # protects Delta _delta_log from accidental overwrite/deletion
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle rule: expire old streaming checkpoints/quarantine data after 90 days
# to keep storage cost bounded -- an easy thing to forget on a long-running
# streaming job's checkpoint directory.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-checkpoints"
    status = "Enabled"
    filter {
      prefix = "_checkpoints/"
    }
    expiration {
      days = 90
    }
  }

  rule {
    id     = "expire-quarantine"
    status = "Enabled"
    filter {
      prefix = "quarantine/"
    }
    expiration {
      days = 180
    }
  }
}

# --- IAM role assumed by Databricks (or EMR / any Spark cluster) to read/write the bucket ---
resource "aws_iam_role" "lakehouse_access" {
  name = "lakehouse-spark-access-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        # Replace with your Databricks workspace's cross-account role ARN,
        # or an EC2/EKS trust policy if not using Databricks.
        AWS = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "lakehouse_rw" {
  name = "lakehouse-rw-${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = "${aws_s3_bucket.lakehouse.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = aws_s3_bucket.lakehouse.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.lakehouse_access.name
  policy_arn = aws_iam_policy.lakehouse_rw.arn
}

output "bucket_name" {
  value = aws_s3_bucket.lakehouse.id
}

output "bucket_arn" {
  value = aws_s3_bucket.lakehouse.arn
}

output "iam_role_arn" {
  value = aws_iam_role.lakehouse_access.arn
}
