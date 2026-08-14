# S3 구성에서만 사용
locals {
  # S3 버킷명
  # 글로벌 기준 : 버킷 이름은 3~63자여야 하며 글로벌 네임스페이스 내에서 고유해야 합니다. 
  #             또한 버킷 이름은 문자나 숫자로 시작하고 끝나야 합니다. 
  #             유효한 문자는 a~z, 0~9, 마침표(.), 하이픈(-)입니다.
  # data.aws_caller_identity.current.account_id : AWS 계정 ID
  airflow_bucket_name = "${var.project_name}-s3-bk-${data.aws_caller_identity.current.account_id}"
}

###############################################
# 버킷 생성
###############################################
resource "aws_s3_bucket" "airflow_data" {
  # 버킷명
  bucket = local.airflow_bucket_name
  # 버킷을 삭제할 때
  # false : 버킷 내부의 object가 남아있다면, terraform destroy 수행 시 버킷삭제 방지(에러발생)
  # true  : 버킷 내부의 object가 남아있어도, terraform destroy 수행 시 버킷삭제
  force_destroy = var.s3_force_destroy

  tags = merge(
    local.common_tags,
    {
      Name = local.airflow_bucket_name
    }
  )
}

###############################################
# S3 Object Ownership
###############################################
resource "aws_s3_bucket_ownership_controls" "airflow_data" {
  bucket = local.airflow_bucket_name

  rule {
    # BucketOwnerEnforced 
    # ACL 비활성화 (타 계정에서 소유불가)
    # 버킷 소유자가 버킷내부 객체의 소유권을 가짐
    # 접근 제어 IAM Policy / Bucket Policy 중심으로 관리한다. -> ACL 방식 X, 계정 소유자의 권한으로 관리
    object_ownership = "BucketOwnerEnforced"
  }
}

###############################################
# S3의 Public Access Block
# airflow(External) -> IAM access key(IAM 인증) -> AWS S3 bucket 접근
# S3는 private으로 관리
###############################################
resource "aws_s3_bucket_public_access_block" "airflow_data" {
  # 대상 버킷
  bucket = local.airflow_bucket_name

  # 설정
  ## 새로운 public acl 설정 차단
  block_public_acls = true
  ## 기존 public acl 무시
  ignore_public_acls = true
  ## public 접근 허용 bucket 정책 생성 차단
  block_public_policy = true
  ## 버킷이 public policy를 가지더라도 public 접근을 제한
  restrict_public_buckets = true
}

###############################################
# S3 Encryption
# 두 개의 개별 암호화 계층으로 객체를 보호
# S3에 저장되는 object를 자동 암호화
###############################################
resource "aws_s3_bucket_server_side_encryption_configuration" "airflow_data" {
  bucket = local.airflow_bucket_name

  rule {
    apply_server_side_encryption_by_default {
      # AES256 = SSE-S3
      # S3가 관리하는 암호화키를 이용하여 객체를 암호화
      sse_algorithm = "AES256"
    }
  }
}