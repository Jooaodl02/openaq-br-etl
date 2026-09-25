# Bronze: database, tabela e o job que roda todo dia.

resource "aws_glue_catalog_database" "bronze" {
  name         = var.database_bronze
  description  = "Camada bronze do ETL OpenAQ"
  location_uri = "s3://${aws_s3_bucket.bucket-etl.bucket}/bronze/"
}

# tabela externa: o Glue guarda so o schema, o dado fica no S3
resource "aws_glue_catalog_table" "openaq_bronze" {
  name          = var.nome_base
  database_name = aws_glue_catalog_database.bronze.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  # o valor vem do caminho no S3 (data_ingestao=YYYY-MM-DD/), entao nao pode
  # aparecer tambem em columns
  partition_keys {
    name = "data_ingestao"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.bucket-etl.bucket}/${var.bronze_prefix}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # bigint e nao int: o pyarrow escreve int64 e o Athena rejeita o arquivo
    # se o tipo declarado nao bater com o fisico
    columns {
      name = "location_id"
      type = "bigint"
    }

    columns {
      name = "sensors_id"
      type = "bigint"
    }

    columns {
      name = "location"
      type = "string"
    }

    columns {
      name = "datetime"
      type = "timestamp"
    }

    columns {
      name = "lat"
      type = "double"
    }

    columns {
      name = "lon"
      type = "double"
    }

    columns {
      name = "parameter"
      type = "string"
    }

    columns {
      name = "units"
      type = "string"
    }

    columns {
      name = "value"
      type = "double"
    }

    columns {
      name = "tipo_ingestao"
      type = "string"
    }
  }
}

module "bronze_processamento_diario" {
  source = "./modules/glue_job"

  job_name       = var.glue_job_name_bronze_diario
  description    = "Le o bruto da API OpenAQ do dia e grava na camada bronze"
  role_arn       = aws_iam_role.glue_job_role.arn
  scripts_bucket = aws_s3_bucket.bucket-etl.id
  scripts_prefix = var.glue_scripts_prefix
  script_path    = "scr/bronze/processamento_diario.py"

  # lidos pelo getResolvedOptions no processamento_diario.py
  job_arguments = {
    "--BUCKET_NAME"     = aws_s3_bucket.bucket-etl.id
    "--API_PREFIX"      = var.api_prefix
    "--BRONZE_PREFIX"   = var.bronze_prefix
    "--DATABASE_BRONZE" = aws_glue_catalog_database.bronze.name
    "--TABLE_NAME"      = aws_glue_catalog_table.openaq_bronze.name
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_job_s3,
  ]
}
