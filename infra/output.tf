# 버킷명
output "s3_bucket_name" {
  description = "s3 bucket name by ariflow"
  value       = local.airflow_bucket_name
}

output "silver_glue_database_name" {
  description = "Silver Glue Database Name"
  value       = aws_glue_catalog_database.silver.name
}
output "silver_glue_table_name" {
  description = "Silver Glue Table Name"
  value       = aws_glue_catalog_table.silver.name
}
output "s3_silver_bucket_name" {
  description = "s3 silver bucket name by ariflow"
  value       = "s3://${var.silver_bucket_name}/silver"
}
