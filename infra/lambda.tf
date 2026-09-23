# Ingestao: le a API OpenAQ e grava o bruto no S3.

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/lambda.py"
  output_path = "${path.module}/lambda.zip"
}

# criado na mao so para poder definir retencao. se a Lambda criar sozinha,
# o log fica guardado para sempre e vira custo
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
  # e o hash que faz o Terraform perceber que o codigo mudou
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

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

  depends_on = [
    aws_iam_role_policy_attachment.lambda_logs,
    aws_iam_role_policy.lambda_openaq,
    aws_cloudwatch_log_group.lambda,
  ]
}

# assume_role_policy diz QUEM veste o papel, as policies abaixo dizem O QUE ele faz
resource "aws_iam_role" "lambda_role" {
  name = "${var.lambda_function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# escreve so no prefixo da API, le so este segredo
resource "aws_iam_role_policy" "lambda_openaq" {
  name = "${var.lambda_function_name}-s3-secrets"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "GravarArquivosDaApi"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.bucket-etl.arn}/${var.api_prefix}/*"
      },
      {
        Sid      = "LerChaveDaApi"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_secretsmanager_secret.openaq_api_key.arn
      }
    ]
  })
}
