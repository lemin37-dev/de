variable "aws_region" {
  description = "AWS 리전"
  type        = string
  default     = "ap-northeast-2"
}

variable "project_name" {
  description = "데이터엔지니어 프로젝트 연습용"
  type        = string
  default     = "de-ai-19-loggen"
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
  default     = false
}

# s3.tf가 없다면 기존에 존재하는 버킷을 사용하여 처리하는 방식
variable "silver_bucket_name" {
  description = "기존 silver parquet 데이터가 실제 저장하고 있는 S3 버킷 이름"
  type        = string
  # default를 생략했을 때 plan or apply시 질의(user interaction) 진행
  default     = "de-ai-19-loggen-s3-bk-827913617635"
}