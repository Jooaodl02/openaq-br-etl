variable "aws_region" {
  type        = string
  description = "Regiao da AWS onde os recursos sao criados"
}

variable "aws_profile" {
  type        = string
  description = "Perfil do ~/.aws/credentials usado para autenticar o Terraform"
}

variable "project_name" {
  type        = string
  description = "Identificador do projeto, aplicado como tag em todos os recursos"
  default     = "etl-openaq"
}

variable "bucket_name" {
  type        = string
  description = "Nome do bucket S3 que guarda as camadas bronze, silver e gold"
}

variable "lambda_function_name" {
  type        = string
  description = "Nome da funcao Lambda que ingere os dados da API OpenAQ"
  default     = "processamento-api-openaq"
}

variable "lambda_handler" {
  type        = string
  description = "Ponto de entrada da Lambda, no formato arquivo.funcao"
  default     = "lambda.lambda_handler"
}

variable "openaq_secret_name" {
  type        = string
  description = "Nome do segredo no Secrets Manager que guarda a chave da API OpenAQ"
}

variable "secret_recovery_window_in_days" {
  type        = number
  description = "Dias de retencao do segredo apos o destroy. 0 exclui na hora"
  default     = 0
}

variable "openaq_location_ids" {
  type        = list(string)
  description = "IDs das estacoes OpenAQ ingeridas pela Lambda"
}

variable "api_prefix" {
  type        = string
  description = "Prefixo onde a Lambda grava o bruto da API. Tambem limita a policy do S3"
  default     = "arquivos_api"
}

variable "bronze_prefix" {
  type        = string
  description = "Prefixo da Bronze: o historico CSV ja convertido para Parquet"
  default     = "bronze/historico_openaq"
}

variable "days_back" {
  type        = string
  description = "Tamanho da janela deslizante em dias. 3 cobre o atraso de ~72h da OpenAQ"
  default     = "3"
}

variable "lambda_timeout" {
  type        = number
  description = "Tempo maximo de execucao da Lambda, em segundos. 900 e o teto da AWS"
  default     = 900
}

variable "lambda_memory_size" {
  type        = number
  description = "Memoria da Lambda em MB. Define tambem a fatia de CPU"
  default     = 256
}

variable "log_retention_days" {
  type        = number
  description = "Dias de retencao dos logs da Lambda no CloudWatch"
  default     = 14
}

variable "nome_base_historico" {
  type        = string
  description = "Nome da base historica no Glue Catalog"
  default     = "historico_openaq"
}

variable "nome_base_api" {
  type        = string
  description = "Nome da base API no Glue Catalog"
  default     = "api_openaq"
}

variable "nome_base" {
  type        = string
  description = "Nome da base API no Glue Catalog"
  default     = "openaq"
}


variable "database_bronze" {
  type        = string
  description = "Nome da base bronze no Glue Catalog"
  default     = "bronze_openaq"
}

variable "database_silver" {
  type        = string
  description = "Nome da base prata no Glue Catalog"
  default     = "silver_openaq"
}

variable "database_gold" {
  type        = string
  description = "Nome da base ouro no Glue Catalog"
  default     = "gold_openaq"
}