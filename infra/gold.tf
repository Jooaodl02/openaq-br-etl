# Gold: database, tabela Iceberg e o job que roda todo dia.

resource "aws_glue_catalog_database" "gold" {
  name         = var.database_gold
  description  = "Camada gold do ETL OpenAQ"
  location_uri = "s3://${aws_s3_bucket.bucket-etl.bucket}/${var.gold_prefix}/"
}

# Iceberg pelo mesmo motivo da silver: a carga diaria entra por MERGE INTO, que
# e row-level operation. o open_table_format_input manda o Glue criar o
# metadata inicial (o metadata.json em gold/openaq/metadata/), senao a tabela
# nasce sem metadata_location e o Spark nao consegue abrir.
#
# sem particao, de proposito, e aqui com mais folga que na silver: a gold e uma
# linha por (id_sensor, anomesdia), algumas centenas por dia contra os milhares
# de leituras da silver. o min/max por arquivo que o Iceberg guarda nos
# manifests ja poda o filtro de anomesdia, porque a escrita e cronologica.
resource "aws_glue_catalog_table" "openaq_gold" {
  name          = var.nome_base
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.bucket-etl.bucket}/${var.gold_prefix}/${var.nome_base}/"

    # as colunas sao o SELECT final do CRUZAMENTO, em
    # scr/gold/processamento_diario.py, na mesma ordem: primeiro as sete do
    # BASE_SILVER_NO_DUP_POR_ANOMESDIA, depois os tres top e os tres bottom.
    #
    # a ordem e os nomes importam: o MERGE grava com INSERT *, que casa coluna
    # por nome. coluna renomeada ou faltando aqui quebra o job na escrita
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

    # anomesdia vem como int da silver, o GROUP BY nao muda o tipo
    columns {
      name = "anomesdia"
      type = "int"
    }

    # os seis saem de silver.valor, que e double. aceitam null: as CTEs entram
    # por LEFT JOIN, entao sensor com menos de tres leituras no dia preenche so
    # o que da (um unico valor vira top1 e bottom1 ao mesmo tempo)
    columns {
      name = "top1_valor"
      type = "double"
    }

    columns {
      name = "top2_valor"
      type = "double"
    }

    columns {
      name = "top3_valor"
      type = "double"
    }

    columns {
      name = "bottom1_valor"
      type = "double"
    }

    columns {
      name = "bottom2_valor"
      type = "double"
    }

    columns {
      name = "bottom3_valor"
      type = "double"
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

module "gold_processamento_diario" {
  source = "./modules/glue_job"

  job_name       = var.glue_job_name_gold_diario
  description    = "Le a silver e grava na gold o top3 e o bottom3 de valor por sensor e dia"
  role_arn       = aws_iam_role.glue_job_role.arn
  scripts_bucket = aws_s3_bucket.bucket-etl.id
  scripts_prefix = var.glue_scripts_prefix
  script_path    = "scr/gold/processamento_diario.py"

  job_arguments = {
    # lidos pelo getResolvedOptions no processamento_diario.py
    "--BUCKET_NAME"     = aws_s3_bucket.bucket-etl.id
    "--DATABASE_SILVER" = aws_glue_catalog_database.silver.name
    "--DATABASE_GOLD"   = aws_glue_catalog_database.gold.name
    "--TABLE_NAME"      = aws_glue_catalog_table.openaq_gold.name

    # dias de anomesdia da silver lidos por execucao. na primeira carga,
    # sobrescrever com --JANELA_DIAS=0 no start-job-run para ler o historico
    "--JANELA_DIAS" = var.gold_janela_dias

    # carrega as libs do Iceberg no classpath do job
    "--datalake-formats" = "iceberg"

    # o Glue aceita um unico --conf, com as chaves separadas por " --conf ".
    # o warehouse aponta para a gold, mas o catalogo glue_catalog resolve as
    # duas tabelas pelo Glue Catalog: o job le glue_catalog.<silver>.openaq e
    # escreve em glue_catalog.<gold>.openaq
    "--conf" = join(" --conf ", [
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
      "spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.bucket-etl.bucket}/${var.gold_prefix}/",
    ])
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_job_s3,
  ]
}
