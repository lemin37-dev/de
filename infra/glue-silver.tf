# 데이터 구조, 위치 등 Meta 데이터를 관리하는 서비스

# AWS Glue Data Catalog > database > 
# 1. 데이터베이스 구성
resource "aws_glue_catalog_database" "silver" {
  name        = "${lower(replace(var.project_name, "-", "_"))}_silver_glue_db"
  description = "Silver parquet 데이터를 athena/airflow에서 조회하기 위한 Glue DB"

  # 실습을 위해 삭제 방지
  lifecycle {
    prevent_destroy = true
  }
}

# 2. 테이블 구성
# 데이터베이스 내부에 여러 개 정의 가능
# s3 silver에 parquet으로 저장할 때 데이터에 대한 논리적인 테이블 정의
# 이 구조를 기반으로 SQL 수행(Athena 등)하여 특정 데이터를 획득 (향후 배치 프로세싱에서 airflow 기반으로 처리)
resource "aws_glue_catalog_table" "silver" {
  # 테이블 명
  name = "silver_logs_tbl"
  # 데이터베이스 소속 설정
  database_name = aws_glue_catalog_database.silver.name
  # 데이터는 Glue 외부(s3)에 존재함을 의미
  table_type = "EXTERNAL_TABLE"
  # 파라미터 지정
  parameters = {
    # 데이터가 glue 외부에 있음을 표현
    EXTERNAL = "TRUE"

    # parquet의 압축 방식
    "parquet.compression" = "SNAPPY"

    # 파티션 활성화 (s3://bucket/silver/year/month/...)
    "projection.enabled" = "true"

    # 파티션 정보 (year, month, day, hour)
    ## year
    "projection.year.type"  = "integer"
    "projection.year.range" = "2026,2040"
    ## month
    ## 1 -> 01, digits = 2 
    "projection.month.type"   = "integer"
    "projection.month.range"  = "1,12"
    "projection.month.digits" = "2"
    ## day
    "projection.day.type"   = "integer"
    "projection.day.range"  = "1,31"
    "projection.day.digits" = "2"
    ## hour
    "projection.hour.type"   = "integer"
    "projection.hour.range"  = "0,23"
    "projection.hour.digits" = "2"

    # 파티션 S3 경로 규칙
    # SQL : ~ where year = '2026'
    # $${} -> ${} 형태로 보내기 위해 escape 처리
    "storage.location.template" = "s3://${aws_s3_bucket.data.bucket}/silver/year=$${year}/month=$${month}/day=$${day}/hour=$${hour}/"
  }
  # 데이터 위치와 파일 형식, 스키마 구성에 대한 정보
  storage_descriptor {
    # 실제 silver layer 상 s3 root 경로
    location = "s3://${aws_s3_bucket.data.bucket}/silver/"

    # S3 파일이 parquet 형식임을 표기
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    # parquet 내부에서 SNAPPY 압축 활용
    compressed = true

    # parquet 파일과 Glue/Athena 등 테이블 간 사이에서 데이터 구조 해석하는 역할
    ser_de_info {
      # 식별을 위한 이름
      name = "silver-parquet"
      # 데이터 해석을 위한 parquet ser_de 라이브러리
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Silver 공통 스키마 (도메인별 동일)
    # column 1개씩 세팅 -> 자동으로 세팅(glue crawler)
    columns {
      name = "schema_version"
      type = "string"
    }
    columns {
      name = "record_type"
      type = "string"
    }
    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "trace_id"
      type = "string"
    }
    columns {
      name = "run_id"
      type = "string"
    }
    columns {
      name = "occurred_at"
      type = "string"
    }
    columns {
      name = "generated_at_utc"
      type = "string"
    }
    columns {
      name = "domain"
      type = "string"
    }
    columns {
      name = "event_type"
      type = "string"
    }

    # silver 중첩 스키마 (중괄호가 중첩된 형태 -> struct)
    columns {
      name = "service"
      type = "struct<name:string,environment:string,instance_id:string>"
    }
    columns {
      name = "client"
      type = "struct<ip:string,user_agent:string,device_id:string>"
    }
    columns {
      name = "request"
      type = "struct<method:string,path:string,request_bytes:bigint>"
    }
    columns {
      name = "response"
      type = "struct<status_code:bigint,latency_ms:bigint,response_bytes:bigint>"
    }
    columns { # 도메인 별로 상이함 (모든 도메인의 키를 등록)
      name = "data"
      type = "struct<user_id:string,session_id:string,product_id:string,category:string,quantity:bigint,unit_price:bigint,currency:string,campaign:string,keyword:string,result_count:bigint,order_id:string,total_amount:bigint,payment_method:string,payment_result:string,transaction_id:string,customer_id:string,account_id:string,channel:string,risk_score:double,amount:bigint,merchant_id:string,merchant_category:string,authorization_result:string,destination_bank:string,destination_account_token:string,transfer_result:string,balance:bigint,auth_method:string,login_result:string,player_id:string,server_region:string,player_level:bigint,ping_ms:bigint,platform:string,match_id:string,mode:string,party_size:bigint,result:string,score:bigint,duration_seconds:bigint,item_id:string,currency_type:string,purchase_result:string,quest_id:string,reward_xp:bigint,reward_gold:bigint,plant_id:string,line_id:string,equipment_id:string,equipment_type:string,message_id:string,temperature_c:double,vibration_mm_s:double,pressure_bar:double,rpm:bigint,state:string,runtime_seconds:bigint,lot_id:string,sample_size:bigint,defect_count:bigint,quality_result:string,alarm_code:string,severity:string,acknowledged:boolean,maintenance_type:string,technician_id:string,downtime_minutes:bigint>"
    }
    columns {
      name = "_silver"
      type = "struct<layer:string,processor:string,schema_version:string,processed_at:string>"
    }
  }
  # partition key 정의
  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }
}
