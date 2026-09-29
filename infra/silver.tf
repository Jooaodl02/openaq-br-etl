# Silver: database, tabela Iceberg e o job que roda todo dia.

resource "aws_glue_catalog_database" "silver" {
  name         = var.database_silver
  description  = "Camada silver do ETL OpenAQ"
  location_uri = "s3://${aws_s3_bucket.bucket-etl.bucket}/${var.silver_prefix}/"
}

# Iceberg, e nao external table como na bronze: a carga diaria e incremental
# via MERGE INTO, que precisa de row-level operation na tabela, e o Iceberg
# resolve isso sem MSCK nem reparticionamento manual. o open_table_format_input
# manda o Glue criar o metadata inicial (o metadata.json em
# silver/openaq/metadata/), senao a tabela nasce sem metadata_location e o
# Spark nao consegue abrir.
resource "aws_glue_catalog_table" "openaq_silver" {
  name          = var.nome_base
  database_name = aws_glue_catalog_database.silver.name
  table_type    = "EXTERNAL_TABLE"

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  # anomesdia primeiro: e o filtro natural das consultas (recorte por dia da
  # medicao). data_ingestao vem depois, para separar o que cada execucao trouxe
  # e permitir reprocessar uma ingestao sem varrer a tabela toda.
  partition_keys {
    name = "anomesdia"
    type = "int"
  }

  partition_keys {
    name = "data_ingestao"
    type = "date"
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.bucket-etl.bucket}/${var.silver_prefix}/${var.nome_base}/"

    # os tipos seguem o select final do scr/silver/processamento_diario.py.
    # bigint nos ids porque vem de bigint na bronze; anomesdia e int porque o
    # job faz cast("int") no date_format
    columns {
      name = "id_localizacao"
      type = "bigint"
    }

    columns {
      name = "id_sensor"
      type = "bigint"
    }

    columns {
      name = "localizacao"
      type = "string"
    }

    columns {
      name = "data_hora"
      type = "timestamp"
    }

    columns {
      name = "latitude"
      type = "double"
    }

    columns {
      name = "longitude"
      type = "double"
    }

    columns {
      name = "parametro"
      type = "string"
    }

    columns {
      name = "parametro_descricao"
      type = "string"
    }

    columns {
      name = "unidade"
      type = "string"
    }

    columns {
      name = "valor"
      type = "double"
    }

    columns {
      name = "tipo_ingestao"
      type = "string"
    }
  }

  # cada commit do Spark troca o metadata_location da tabela. sem o ignore, todo
  # plan depois de uma execucao acusa drift e quer reverter para o metadata que
  # o Terraform criou, o que apagaria o historico de snapshots
  lifecycle {
    ignore_changes = [
      parameters,
      storage_descriptor[0].columns,
    ]
  }
}

module "silver_processamento_diario" {
  source = "./modules/glue_job"

  job_name       = var.glue_job_name_silver_diario
  description    = "Le a bronze, traduz e deduplica, e grava na camada silver"
  role_arn       = aws_iam_role.glue_job_role.arn
  scripts_bucket = aws_s3_bucket.bucket-etl.id
  scripts_prefix = var.glue_scripts_prefix
  script_path    = "scr/silver/processamento_diario.py"

  job_arguments = {
    # lidos pelo getResolvedOptions no processamento_diario.py
    "--BUCKET_NAME"     = aws_s3_bucket.bucket-etl.id
    "--DATABASE_BRONZE" = aws_glue_catalog_database.bronze.name
    "--DATABASE_SILVER" = aws_glue_catalog_database.silver.name
    "--TABLE_NAME"      = aws_glue_catalog_table.openaq_silver.name

    # dias de data_ingestao da bronze lidos por execucao. na primeira carga,
    # sobrescrever com --JANELA_DIAS=0 no start-job-run para ler o historico
    "--JANELA_DIAS" = var.silver_janela_dias

    # carrega as libs do Iceberg no classpath do job
    "--datalake-formats" = "iceberg"

    # o Glue aceita um unico --conf, com as chaves separadas por " --conf ".
    # glue_catalog e o nome do catalogo dentro do Spark: o script escreve em
    # glue_catalog.<database>.<tabela>
    "--conf" = join(" --conf ", [
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
      "spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.bucket-etl.bucket}/${var.silver_prefix}/",
    ])
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_job_s3,
  ]
}
