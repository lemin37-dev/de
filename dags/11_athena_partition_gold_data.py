# Silver -> Gold (운영형 / 데이터는 파티션 단위로 insert 처리 - 일간 집계 데이터 등 -> 기존 데이터 및 테이블 스키마를 삭제하지 않음) 

# 1. 모듈 가져오기
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
# 하루에 여러번 수행 시 기존 데이터가 대체되어야 하는 경우 활용
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

# 2. 환경변수
AWS_CONN_ID       = "aws_default"
BUCKET_NAME       = "de-ai-19-loggen-s3-bk-827913617635"
DATABASE_NAME     = "de_ai_19_loggen_silver_glue_db"
SILVER_TABLE_NAME = "silver_logs_tbl"
# 1회성 테이블 (다음번 batch 작업 시 삭제 후 다시 신규 생성)
GOLD_TABLE_NAME   = "gold_daily_report_tbl"
# Athena SQL 실행 결과 저장 -> 작업 그룹에 의해 저장되는 위치가 결정 or 직접 경로 지정
QUERY_RESULT_S3   = f"s3://{BUCKET_NAME}/athena/dags/"
# CTAS가 실제로 참조하는 데이터 location -> parquet으로 저장
GOLD_PREFIX       = "gold/daily_report/"
GOLD_LOCATION     = f"s3://{BUCKET_NAME}/{GOLD_PREFIX}"
# 처리 대상 날짜/시간 세팅
TARGET_DATE       = "2026-08-26" #"{{dag_run.conf.get('target_date', ds)}}" 
TARGET_YEAR       = "2026" # "{{dag_run.conf.get('target_date', ds)[0:4]}}" 
TARGET_MONTH      = "08" #"{{dag_run.conf.get('target_date', ds)[5:7]}}" 
TARGET_DAY        = "26" #"{{dag_run.conf.get('target_date', ds)[8:10]}}" 

# 매일 1개의 데이터셋 구성 -> 파티션 사용 권장
# s3://bucket/gold/daily_report/year=2026/month=08/day=26/
GOLD_PARTITON_PREFIX = f"{GOLD_PREFIX}year={TARGET_YEAR}/month={TARGET_MONTH}/day={TARGET_DAY}/"

# 3. DAG 정의
with DAG(
  dag_id            = "11_athena_partition_gold_data",
  description       = "Silver -> Partition 단위 Gold (parquet) 생성",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = "0 5 * * *",
  start_date        = pendulum.datetime(2026, 6, 29, tz=pendulum.timezone("Asia/Seoul")),
  catchup           = False,
  tags              = ['aws', 'athena', 'partition']
) as dag:
  # 4. Task 정의
  # 4-1. Gold 테이블 생성 (없을 때만)
  t1_create_gold_table = AthenaOperator(
    task_id = "create_gold_table",
    query   = f'''
      create external table if not exists {GOLD_TABLE_NAME} (
          report_date         	date,  
          domain              	string,
          event_type          	string,
          service_name        	string,
          total_count         	int,   
          response_count      	bigint,
          success_count       	bigint,
          error_count         	bigint,
          error_rate_pct      	double,
          avg_latency_ms      	double,
          min_latency_ms      	bigint,
          p95_latency_ms      	bigint,
          max_latency_ms      	bigint,
          total_request_bytes 	bigint,
          total_response_bytes	bigint
      )
      partitioned by (
          year    STRING,
          month   STRING,
          day     STRING
      )
      STORED AS PARQUET
      LOCATION '{GOLD_LOCATION}'
    ''',
    # 접속 및 DB 정보
    aws_conn_id     = AWS_CONN_ID,
    database        = DATABASE_NAME,
    output_location = QUERY_RESULT_S3,
  )
  # 4-2. 동일날짜에 중복 실행될 경우 스키마 파티션 삭제
  t2_drop_partition = AthenaOperator(
    task_id = "drop_partition",
    query   = f'''
      alter table {GOLD_TABLE_NAME}
      drop if exists Partition (
        year  = '{TARGET_YEAR}',
        month = '{TARGET_MONTH}',
        day   = '{TARGET_DAY}'
      )
    ''',
    # 접속 및 DB 정보
    aws_conn_id     = AWS_CONN_ID,
    database        = DATABASE_NAME,
    output_location = QUERY_RESULT_S3,
  )
  # 4-3. 동일날짜에 중복 실행될 경우 S3 삭제
  t3_delete_gold_s3 = S3DeleteObjectsOperator(
    task_id = "delete_gold_s3",
    bucket = BUCKET_NAME,
    prefix = GOLD_PARTITON_PREFIX,
    aws_conn_id = AWS_CONN_ID
  )
  # 4-4. 당일 전체 데이터에 대한 insert 처리
  t4_insert_gold_table = AthenaOperator(
    task_id = "insert_gold_table",
    query   = f'''
      insert into {GOLD_TABLE_NAME}
      select
            DATE('{TARGET_DATE}') AS report_date,
            domain,
            COALESCE( event_type, 'unknown') AS event_type,
            COALESCE( service.name, 'unknown') AS service_name,
            COUNT(*) AS total_count,
            COUNT(response.status_code) AS response_count,
            COUNT_IF( response.status_code >= 200 AND response.status_code < 400 ) AS success_count,
            COUNT_IF( response.status_code >= 400 ) AS error_count,
            CASE
                WHEN COUNT(response.status_code) = 0 THEN 0
                ELSE ROUND( 100.0 * COUNT_IF(response.status_code >= 400) / COUNT(response.status_code), 2 )
            END
                AS error_rate_pct,
            ROUND( AVG( CAST( response.latency_ms AS DOUBLE ) ), 2 ) AS avg_latency_ms,
            MIN(response.latency_ms) AS min_latency_ms,
            APPROX_PERCENTILE( response.latency_ms, 0.95 ) AS p95_latency_ms,
            MAX(response.latency_ms) AS max_latency_ms,
            COALESCE( SUM(request.request_bytes), 0 ) AS total_request_bytes,
            COALESCE( SUM(response.response_bytes), 0 ) AS total_response_bytes,
            '{TARGET_YEAR}' as year,
            '{TARGET_MONTH}' as month,
            '{TARGET_DAY}' as day
      from {SILVER_TABLE_NAME}
      where year = '{TARGET_YEAR}'
        and month = '{TARGET_MONTH}'
        and day = '{TARGET_DAY}'
      group by
            domain,
            event_type,
            service.name
    ''',
    # 접속 및 DB 정보
    aws_conn_id     = AWS_CONN_ID,
    database        = DATABASE_NAME,
    output_location = QUERY_RESULT_S3,
  )

  # 5. 의존성
  t1_create_gold_table >> t2_drop_partition >> t3_delete_gold_s3 >> t4_insert_gold_table
