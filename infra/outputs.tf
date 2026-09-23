output "lambda_function_name" {
  description = "Nome da funcao, usado no aws lambda invoke"
  value       = aws_lambda_function.openaq_ingestao.function_name
}

output "openaq_secret_arn" {
  description = "ARN do segredo com a chave da API OpenAQ"
  value       = aws_secretsmanager_secret.openaq_api_key.arn
}

output "glue_job_diario_name" {
  description = "Nome do job da bronze, usado no aws glue start-job-run"
  value       = module.bronze_processamento_diario.job_name
}
