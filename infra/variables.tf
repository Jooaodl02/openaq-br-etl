# --- geral ---

variable "aws_region" {
  type        = string
  description = "Regiao onde tudo e criado"
}

variable "aws_profile" {
  type        = string
  description = "Perfil do ~/.aws/credentials usado pelo Terraform"
}

variable "project_name" {
  type        = string
  description = "Vira tag em todos os recursos"
  default     = "etl-openaq"
}

variable "bucket_name" {
  type        = string
  description = "Bucket que guarda as camadas bronze, silver e gold"
}

# --- prefixos no bucket ---

variable "api_prefix" {
  type        = string
  description = "Onde a Lambda grava o bruto da API"
  default     = "arquivos_api"
}

variable "bronze_prefix" {
  type        = string
  description = "Prefixo da bronze"
  default     = "bronze"
}

variable "silver_prefix" {
  type        = string
  description = "Prefixo da silver"
  default     = "silver"
}

variable "gold_prefix" {
  type        = string
  description = "Prefixo da gold"
  default     = "gold"
}

# --- Lambda de ingestao ---

variable "lambda_function_name" {
  type        = string
  description = "Nome da funcao que ingere a API OpenAQ"
  default     = "processamento-api-openaq"
}

variable "lambda_handler" {
  type        = string
  description = "Ponto de entrada, no formato arquivo.funcao"
  default     = "lambda.lambda_handler"
}

variable "openaq_location_ids" {
  type        = list(string)
  description = "IDs das estacoes OpenAQ ingeridas"
}

variable "days_back" {
  type        = string
  description = "Janela deslizante em dias. 3 cobre o atraso de ~72h da OpenAQ"
  default     = "3"
}

variable "lambda_timeout" {
  type        = number
  description = "Timeout em segundos. 900 e o teto da AWS"
  default     = 900
}

variable "lambda_memory_size" {
  type        = number
  description = "Memoria em MB. Define tambem a fatia de CPU"
  default     = 256
}

variable "log_retention_days" {
  type        = number
  description = "Retencao dos logs da Lambda no CloudWatch"
  default     = 14
}

# --- Secrets Manager ---

variable "openaq_secret_name" {
  type        = string
  description = "Nome do segredo com a chave da API OpenAQ"
}

variable "secret_recovery_window_in_days" {
  type        = number
  description = "Retencao do segredo apos o destroy. 0 exclui na hora"
  default     = 0
}

# --- Glue Catalog ---

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
  description = "Base bronze no Glue Catalog"
  default     = "bronze_openaq"
}

variable "database_silver" {
  type        = string
  description = "Base silver no Glue Catalog"
  default     = "silver_openaq"
}

variable "database_gold" {
  type        = string
  description = "Base gold no Glue Catalog"
  default     = "gold_openaq"
}

# --- jobs do Glue ---

variable "glue_role_name" {
  type        = string
  description = "Role compartilhada pelos jobs das tres camadas"
  default     = "glue-job-role"
}

variable "glue_scripts_prefix" {
  type        = string
  description = "Onde os scripts .py dos jobs sao publicados no bucket"
  default     = "scripts"
}

variable "glue_job_name_bronze_diario" {
  type        = string
  description = "Nome do job que processa a ingestao diaria na bronze"
  default     = "bronze-processamento-diario"
}
