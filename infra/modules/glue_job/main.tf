locals {
  # scr/bronze/processamento_diario.py -> scripts/bronze/processamento_diario.py
  script_key = "${var.scripts_prefix}/${trimprefix(var.script_path, "scr/")}"

  # path.root e a pasta infra/, de onde o terraform roda
  script_source = "${path.root}/../${var.script_path}"
}

resource "aws_s3_object" "script" {
  bucket = var.scripts_bucket
  key    = local.script_key
  source = local.script_source

  # sem o hash o Terraform so olha a chave e nunca republica o script alterado
  etag = filemd5(local.script_source)
}

resource "aws_glue_job" "this" {
  name              = var.job_name
  description       = var.description
  role_arn          = var.role_arn
  glue_version      = var.glue_version
  max_retries       = var.max_retries
  timeout           = var.timeout
  number_of_workers = var.number_of_workers
  worker_type       = var.worker_type
  execution_class   = "STANDARD"

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/${local.script_key}"
    python_version  = "3"
  }

  notification_property {
    notify_delay_after = var.notify_delay_after
  }

  # os argumentos da camada entram por ultimo e vencem em caso de conflito
  default_arguments = merge(
    {
      "--job-language"                     = "python"
      "--continuous-log-logGroup"          = "/aws-glue/jobs"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-continuous-log-filter"     = "true"
      "--enable-metrics"                   = ""
      "--enable-auto-scaling"              = "true"

      # sem isso o saveAsTable cai no metastore local do Spark: o job passa
      # e nada aparece no Athena
      "--enable-glue-datacatalog" = "true"
    },
    var.job_arguments,
  )

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }

  depends_on = [aws_s3_object.script]
}
