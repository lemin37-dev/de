'''
- 관련 예시
  - 쏘카 : 렌터카 반납 -> 사진 촬영 업로드(S3) -> 트리거(변화) -> 이미지 판독(파손여부 체크 등) : 분석 -> 판정
- 동작
  - 버킷 내 특정 공간 감시 -> 파일 업로드 동작 -> 감지 -> DAG의 Task 작동
'''
# 모듈
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # key 감시용
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator # 특정 객체 삭제
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pendulum

# 환경변수
BUCKET_NAME      = "de-ai-19-infra-s3-bk-827913617635"
UPLOAD_FILE_NAME = "sensor_data.csv"
S3_KEY           = f'airflow/{UPLOAD_FILE_NAME}'  # S3상의 경로 조정
KST              = pendulum.timezone("Asia/Seoul")

# 콜백함수
def _read_data(**kwargs):
  # S3 hook 사용
  hook = S3Hook(aws_conn_id="aws_default")  # 연결
  # 읽기
  data = hook.read_key(bucket_name=BUCKET_NAME, key=S3_KEY)
  # 비즈니스 로직 수행
  logging.info("--- 내용 출력 시작 ---")
  logging.info(data)
  logging.info("--- 내용 출력 종료 ---")
  pass

# DAG
with DAG(
  dag_id            = "09_aws_s3_consumer",
  description       = "S3 버킷 내 특정 위치를 감시, 감지된 이후 처리",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = '@daily', # None이면 아예 돌아가지 않기 때문에 최소한의 스케줄은 필요
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['aws', 's3', 'consumer']
) as dag:
  # 감지
  task_wait_for_trigger = S3KeySensor(
    task_id     = "wait_for_trigger",
    # 감지 대상 설정
    bucket_name = BUCKET_NAME,
    bucket_key  = S3_KEY,
    aws_conn_id = "aws_default",
    # 감지 방법 (지속적 감시)
    mode          = "reschedule", # 감시 대기 중 자원은 반납
    poke_interval = 10,           # 10초 간격으로 체크 
    timeout       = 60*10         # 가동 후 10분 넘게 감지가 안되면 종료
  )
  # 읽기 (감지가 되었을 때)
  task_read_data = PythonOperator(
    task_id         = "read_data",
    python_callable = _read_data
  )
  # 삭제 (실제 적용 시 백업처리하도록 구성하기)
  task_delete_data = S3DeleteObjectsOperator(
    task_id     = "delete_data",
    bucket      = BUCKET_NAME,
    keys        = [S3_KEY],
    aws_conn_id = "aws_default"
  )

  # 의존성
  task_wait_for_trigger >> task_read_data >> task_delete_data