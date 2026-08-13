'''
- DAG -> DAG 작동시키는 트리거 오퍼레이터 사용
'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
import pendulum
import json
import random
import pandas as pd
import os

# 전역변수
KST = pendulum.timezone("Asia/Seoul")
DATA_PATH = "/opt/airflow/dags/data"
os.makedirs(DATA_PATH, exist_ok = True)

def _extract_cb(**kwargs):
  '''
    - 스마트팩토리에 설치된 오븐 센서에서 데이터가 발생, 데이터 레이크(S3)에 저장되어 있다고 가정
  '''
  # 더미데이터 구성
  data = [
    {
      "sensor_id"   : f"SENSOR_{i+1}", # 장비 ID
      "timestamp"   : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # 데이터 생성시간
      "temperature" : round(random.uniform(20.0, 150.0), 2),   # 온도 허용 범위
      "status"      : "on"  # on or off
    }
    for i in range(10)
  ]
  # 데이터를 다음 Task에 전달 -> 데이터 전달 or 데이터 저장(리눅스 경로 상) 후 path 전달
  # 파일명 sensor_data_20260813.json  : 20260813 -> "ds_nodash" 활용
  file_full_path = f"{DATA_PATH}/sensor_data_{kwargs['ds_nodash']}.json"
  with open(file_full_path, 'w') as f:
    json.dump(data, f)

  logging.info(f'Extract 데이터 경로 {file_full_path}')
  logging.info(f'Extract 데이터 {data}')
  # XCom을 통해 전달
  return file_full_path
  

with DAG(
  dag_id            = "06_multi_dag_1_extract",
  description       = "Extract 전용 DAG",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = "@daily",
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['extract', 'ETL']
) as dag:
  task_extract      = PythonOperator(
    task_id = "extract",
    python_callable = _extract_cb
  )
  task_trigger_transform_dag_run = TriggerDagRunOperator(
    task_id = "trigger_transform_dag_run",
    trigger_dag_id = "06_multi_dag_2_transform",
    # 구동 시 전달시킬 데이터 -> XCom 통해 획득
    conf = {
      "json_path" : "{{task_instance.xcom_pull(task_ids='extract')}}" 
    },
    # DAG 최초 수행시간 세팅 -> 첫번째 DAG과 동일하게 두번째 DAG도 해당 시간으로 최초 수행시간으로 간주할지
    reset_dag_run = True,
    # 기타 설정
    # 다음 DAG가 수행되는 것을 보고(대기) 종료할 것인가?
    wait_for_completion = False  # 명령 전달 후 바로 종료
  )

  task_extract >> task_trigger_transform_dag_run
