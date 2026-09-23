# bucket unico, as camadas sao prefixos dentro dele
resource "aws_s3_bucket" "bucket-etl" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "versioning_example" {
  bucket = aws_s3_bucket.bucket-etl.id

  versioning_configuration {
    status = "Enabled"
  }
}
