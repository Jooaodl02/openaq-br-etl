# Silver: database, tabela Iceberg e o job que roda todo dia.

resource "aws_glue_catalog_database" "silver" {
  name         = var.database_silver
  description  = "Camada silver do ETL OpenAQ"
  location_uri = "s3://${aws_s3_bucket.bucket-etl.bucket}/${var.silver_prefix}/"
}

# Iceberg, e nao external table como na bronze: a carga diaria e incremental
# via MERGE INTO, que precisa de row-level operation na tabela. o
# open_table_format_input manda o Glue criar o metadata inicial (o
# metadata.json em silver/openaq/metadata/), senao a tabela nasce sem
# metadata_location e o Spark nao consegue abrir.
#
# sem particao, de proposito. a tabela inteira cabe em algumas dezenas de MB
# (1,2 mi de linhas no historico, poucos milhares por dia), e o Iceberg guarda
# min/max de cada coluna por arquivo nos manifests: como a escrita e
# cronologica, um filtro por anomesdia ja pula arquivo pelo estatistico.
# declarar particao aqui daria centenas de arquivos de dezenas de KB e um
# metadado do tamanho do dado. se o volume crescer, "ALTER TABLE ... ADD
# PARTITION FIELD anomesdia" particiona sem reescrever o que ja existe.
#
# o Glue tambem nao aceitaria: CreateTable recusa PartitionKeys em tabela
# Iceberg, a spec de particao so entra por DDL do Spark ou do Athena.
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

    # anomesdia sai de data_hora e data_ingestao vem da bronze. sao colunas
    # comuns: no Iceberg nao existe a separacao entre coluna e chave de
    # particao que a bronze tem
    columns {
      name = "anomesdia"
      type = "int"
    }

    columns {
      name = "data_ingestao"
      type = "date"
    }
  }

  # daqui pra frente a tabela e do Iceberg, nao do Terraform: cada commit do
  # Spark reescreve o metadata_location nos parameters e o espelho das colunas
  # no storage_descriptor. sem o ignore, todo plan depois de uma execucao quer
  # reverter para o que este arquivo diz e apagaria o historico de snapshots.
  # o bloco acima e o bootstrap da tabela; mudanca de schema depois disso vai
  # por ALTER TABLE, nao por apply
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
