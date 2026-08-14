'''
- airflow aws를 액세스 -> 오퍼레이터 등 도구 제공 -> 패키지 설치
  - docker-compose.yaml
    - _PIP_ADDITIONAL_REQUIREMENTS: ... apache-airflow-providers-amazon

- 원격 PC에서 AWS S3의 특정 버킷에 간단하게 데이터 업로드 테스트 DAG
'''
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
LOCAL_PATH       = f"/opt/airflow/dags/data/{UPLOAD_FILE_NAME}"
KST              = pendulum.timezone("Asia/Seoul")

# 콜백함수 정의
def _check_s3(**kwargs):
  # S3Hook을 이용하여 실제 업로드 체크
  # hook 생성
  hook = S3Hook(aws_conn_id="aws_default")
  # hook을 이용해 key 조회
  keys = hook.list_keys(bucket_name=BUCKET_NAME)
  # key 체크
  if not keys:
    raise ValueError("버킷 내 키가 없음. 업로드 실패")
  # 데이터 체크
  if UPLOAD_FILE_NAME in keys:
    logging.info(f"{UPLOAD_FILE_NAME} S3 서버 내에 정상 업로드")
  
  pass

# DAG 정의
with DAG(
  dag_id            = "08_aws_s3_basic",
  description       = "S3 단순 업로드",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = "@daily",
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['aws', 's3']
) as dag:
  # Task 정의
  task_upload_to_s3 = LocalFilesystemToS3Operator(
    task_id     = "upload_to_s3",
    filename    = LOCAL_PATH,       # Local 파일경로
    dest_key    = UPLOAD_FILE_NAME, # S3 버킷 내에서 객체간 구분하는 키
    dest_bucket = BUCKET_NAME,      # 버킷명
    aws_conn_id = "aws_default",    # aws 접속 정보
    replace     = True              # 키가 동일할 때 대체 여부
  )
  task_check_s3     = PythonOperator(
    task_id         = "check_s3",
    python_callable = _check_s3
  )

  # 의존성
  task_upload_to_s3 >> task_check_s3