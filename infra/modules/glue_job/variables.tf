variable "job_name" {
  type        = string
  description = "Nome do job no Glue"
}

variable "description" {
  type        = string
  description = "Descricao que aparece no console"
  default     = ""
}

variable "role_arn" {
  type        = string
  description = "Role que o job veste ao executar"
}

variable "scripts_bucket" {
  type        = string
  description = "Bucket onde o script e publicado"
}

variable "script_path" {
  type        = string
  description = "Caminho do script a partir da raiz do repo (ex: scr/bronze/processamento_diario.py)"
}

variable "scripts_prefix" {
  type        = string
  description = "Prefixo no bucket onde os scripts vao"
  default     = "scripts"
}

variable "job_arguments" {
  type        = map(string)
  description = "Argumentos do job, lidos pelo getResolvedOptions. Vencem os padroes em caso de conflito"
  default     = {}
}

# afinacao: os defaults servem para as tres camadas, a chamada so passa o que foge

variable "glue_version" {
  type        = string
  description = "Runtime do Glue. A 5.0 usa Spark 3.5 e Python 3.11"
  default     = "5.0"
}

variable "worker_type" {
  type        = string
  description = "G.1X = 4 vCPU e 16 GB, menor opcao para Spark"
  default     = "G.1X"
}

variable "number_of_workers" {
  type        = number
  description = "Teto de workers, nao um fixo: o auto scaling esta ligado"
  default     = 2
}

variable "timeout" {
  type        = number
  description = "Timeout em minutos. Corta job travado antes de virar custo"
  default     = 60
}

variable "max_concurrent_runs" {
  type        = number
  description = "1 evita duas escritas na mesma particao"
  default     = 1
}

variable "max_retries" {
  type        = number
  description = "0 porque a escrita e append: retry duplicaria linhas"
  default     = 0
}

variable "notify_delay_after" {
  type        = number
  description = "Minutos ate o Glue avisar que o job esta demorando"
  default     = 30
}
