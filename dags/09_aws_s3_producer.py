'''
- 목표
  - 데이터 생성 -> CSV 저장 -> S3 업로드
  - 스케줄링, 주기적 처리
'''
# 모듈 가져오기
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import pendulum

# 환경변수
BUCKET_NAME      = "de-ai-19-infra-s3-bk-827913617635"
UPLOAD_FILE_NAME = "sensor_data.csv"
S3_KEY           = f'airflow/{UPLOAD_FILE_NAME}'  # S3상의 경로 조정
LOCAL_PATH       = f"/opt/airflow/dags/data/{UPLOAD_FILE_NAME}"
KST              = pendulum.timezone("Asia/Seoul")

# DAG 정의
with DAG(
  dag_id            = "09_aws_s3_producer",
  description       = "생성된 데이터를 S3에 주기적 업로드",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = None,
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['aws', 's3', 'producer']
) as dag:
  # 데이터 생성
  task_create_dummy_data_csv = BashOperator(
    task_id      = "create_dummy_data_csv",
    bash_command = f'echo "id,timestamp,value\n1,$(date),100\n2,$(date),500" > {LOCAL_PATH}'
  )
  # Task 정의
  task_upload_to_s3 = LocalFilesystemToS3Operator(
    task_id     = "upload_to_s3",
    filename    = LOCAL_PATH,       # Local 파일경로
    dest_key    = S3_KEY,           # S3 버킷 내에서 객체간 구분하는 키
    dest_bucket = BUCKET_NAME,      # 버킷명
    aws_conn_id = "aws_default",    # aws 접속 정보
    replace     = True              # 키가 동일할 때 대체 여부
  )

  # 의존성
  task_create_dummy_data_csv >> task_upload_to_s3