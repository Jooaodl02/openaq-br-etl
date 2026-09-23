# so o cofre vem daqui. o valor entra pela CLI, para nao parar no tfstate:
#
#   aws secretsmanager put-secret-value \
#     --secret-id openaq/api_key \
#     --secret-string '{"api_key":"SUA_CHAVE"}' \
#     --region sa-east-1 --profile tf_etl_jooaodl
resource "aws_secretsmanager_secret" "openaq_api_key" {
  name        = var.openaq_secret_name
  description = "Chave da API OpenAQ v3 usada pela Lambda de ingestao"

  recovery_window_in_days = var.secret_recovery_window_in_days
}
