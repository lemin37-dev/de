# 쿼리의 결과를 보관할 s3 경로 정의
locals {
  athena_result_location = "s3://${var.silver_bucket_name}/athena/results"
}

# workgroup
resource "aws_athena_workgroup" "analysis" {
  # workgroup name
  name = "${var.project_name}-analysis"
  # 활성화 여부
  state = "ENABLED"
  # 작업 그룹의 구성
  configuration {
    enforce_workgroup_configuration = true  # 강제적용
    result_configuration {
      output_location = local.athena_result_location
    }
  }
  tags = {
    Processing = "batch"
  }
}