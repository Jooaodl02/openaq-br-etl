# Identidade que a Lambda assume ao executar.
#
# Sao duas coisas distintas:
#   - assume_role_policy: QUEM pode vestir esse papel (o servico Lambda)
#   - policies anexadas:  O QUE quem veste o papel pode fazer
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

# Policy gerenciada pela AWS: permite escrever no CloudWatch Logs.
# Sem ela a funcao roda, mas voce fica sem log nenhum para depurar.
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Permissoes especificas deste job, no menor escopo possivel:
# gravar apenas dentro do prefixo do bruto da API e ler apenas este segredo.
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
