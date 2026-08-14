variable "aws_region" {
  description = "AWS 리전"
  type        = string
  default     = "ap-northeast-2"
}

variable "project_name" {
  description = "데이터엔지니어 프로젝트 연습용"
  type        = string
  default     = "de-ai-19-infra"
}

variable "environment" {
  description = "환경 구분"
  type        = string
  default     = "dev"
}

# S3 버킷을 삭제할 때, 버킷 내부의 객체가 있을 경우 삭제 여부
variable "s3_force_destroy" {
  description = "True = 버킷 내부데이터 및 버킷 삭제"
  type        = bool
  default     = true
}