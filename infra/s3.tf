# Bucket que armazena as camadas do ETL (bronze, silver e gold)
resource "aws_s3_bucket" "bucket-etl" {
  bucket = var.bucket_name
}

# Versionamento: protege contra sobrescrita acidental de um arquivo ja gravado
resource "aws_s3_bucket_versioning" "versioning_example" {
  bucket = aws_s3_bucket.bucket-etl.id

  versioning_configuration {
    status = "Enabled"
  }
}
