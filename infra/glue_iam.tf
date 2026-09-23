# Role unica dos jobs das tres camadas.
#
# uma so, e nao uma por camada: em troca de menos recursos, qualquer job pode
# escrever em qualquer camada. para isolar, seria uma role por job.
resource "aws_iam_role" "glue_job_role" {
  name = var.glue_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# da acesso ao Catalog e aos logs. o S3 dela so cobre bucket aws-glue-*,
# por isso a policy abaixo
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_job_s3" {
  name = "${var.glue_role_name}-s3"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListarOBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.bucket-etl.arn
      },
      {
        Sid    = "LerScriptsEAsCamadasDeOrigem"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.bucket-etl.arn}/${var.glue_scripts_prefix}/*",
          "${aws_s3_bucket.bucket-etl.arn}/${var.api_prefix}/*",
          "${aws_s3_bucket.bucket-etl.arn}/${var.bronze_prefix}/*",
          "${aws_s3_bucket.bucket-etl.arn}/${var.silver_prefix}/*",
          "${aws_s3_bucket.bucket-etl.arn}/${var.gold_prefix}/*",
        ]
      },
      {
        Sid    = "EscreverNasCamadas"
        Effect = "Allow"
        # DeleteObject cobre os temporarios que o Spark limpa ao fechar a particao
        Action = ["s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${aws_s3_bucket.bucket-etl.arn}/${var.bronze_prefix}/*",
          "${aws_s3_bucket.bucket-etl.arn}/${var.silver_prefix}/*",
          "${aws_s3_bucket.bucket-etl.arn}/${var.gold_prefix}/*",
        ]
      }
    ]
  })
}
