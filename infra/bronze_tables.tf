resource "aws_glue_catalog_database" "bronze" {
  name         = var.database_bronze
  description  = "Camada bronze do ETL OpenAQ"
  location_uri = "s3://${aws_s3_bucket.bucket-etl.bucket}/bronze/"
}


# Historico CSV ja convertido para Parquet.
# Tabela externa: o Glue so guarda o schema, os dados ficam no S3.
resource "aws_glue_catalog_table" "openaq_bronze" {
  name          = var.nome_base
  database_name = aws_glue_catalog_database.bronze.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  # coluna de particao: o valor vem do caminho no S3 (data_ingestao=YYYY-MM-DD/),
  # por isso ela NAO pode aparecer tambem em columns
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

    # bigint, e nao int: pandas/pyarrow escrevem int64 por padrao e o Athena
    # rejeita o arquivo se o tipo declarado nao bater com o tipo fisico
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
