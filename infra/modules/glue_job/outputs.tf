output "job_name" {
  description = "Nome do job, usado no aws glue start-job-run"
  value       = aws_glue_job.this.name
}

output "job_arn" {
  description = "ARN do job, para trigger ou agendamento"
  value       = aws_glue_job.this.arn
}

output "script_key" {
  description = "Chave do script publicado no bucket"
  value       = aws_s3_object.script.key
}
