# Segredo que guarda a chave da API OpenAQ.
#
# O Terraform cria apenas o "cofre", nunca o valor. A chave e gravada uma unica
# vez pela CLI, para nao passar pelo terraform.tfstate:
#
#   aws secretsmanager put-secret-value \
#     --secret-id openaq/api_key \
#     --secret-string '{"api_key":"SUA_CHAVE"}' \
#     --region sa-east-1 --profile tf_etl_jooaodl
resource "aws_secretsmanager_secret" "openaq_api_key" {
  name        = var.openaq_secret_name
  description = "Chave da API OpenAQ v3 usada pela Lambda de ingestao"

  # 0 = exclusao imediata no destroy. O padrao (30 dias) agenda a exclusao e
  # impede recriar um segredo com o mesmo nome nesse periodo.
  recovery_window_in_days = var.secret_recovery_window_in_days
}

# ARN do segredo, usado na policy da Lambda na proxima parte
output "openaq_secret_arn" {
  description = "ARN do segredo com a chave da API OpenAQ"
  value       = aws_secretsmanager_secret.openaq_api_key.arn
}
