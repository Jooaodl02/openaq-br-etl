# Empacota o codigo Python num .zip.
# path.module resolve a partir da pasta infra/, e nao do diretorio onde o
# terraform foi chamado. O .zip e artefato de build e esta no .gitignore.
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/lambda.py"
  output_path = "${path.module}/lambda.zip"
}

# Grupo de log criado explicitamente para poder definir a retencao.
# Se a Lambda criar sozinha, os logs ficam guardados para sempre e viram custo.
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "openaq_ingestao" {
  function_name = var.lambda_function_name
  role          = aws_iam_role.lambda_role.arn
  handler       = var.lambda_handler
  runtime       = "python3.11"

  filename = data.archive_file.lambda_zip.output_path
  # hash do conteudo: e o que faz o Terraform perceber que o codigo mudou
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  # o default de 3s nao cobre dezenas de chamadas HTTP em sequencia
  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory_size

  # lidas pelo os.environ no lambda.py
  environment {
    variables = {
      BUCKET_NAME  = aws_s3_bucket.bucket-etl.id
      SECRET_NAME  = aws_secretsmanager_secret.openaq_api_key.name
      LOCATION_IDS = join(",", var.openaq_location_ids)
      PREFIX       = var.api_prefix
      DAYS_BACK    = var.days_back
    }
  }

  # garante que as permissoes e o log group existam antes da funcao
  depends_on = [
    aws_iam_role_policy_attachment.lambda_logs,
    aws_iam_role_policy.lambda_openaq,
    aws_cloudwatch_log_group.lambda,
  ]
}

output "lambda_function_name" {
  description = "Nome da funcao, usado no aws lambda invoke"
  value       = aws_lambda_function.openaq_ingestao.function_name
}
