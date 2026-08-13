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

def _transform_cb(**kwargs):
  # 앞선 DAG가 전달한 내용을 XCom을 통해 획득
  dag_run = kwargs["dag_run"]
  json_file_path = dag_run.conf.get('json_path')
  logging.info(f'전달받은 데이터 파일 경로 {json_file_path}')
  
  # 1) json file -> load -> Dataframe
  df = pd.read_json(json_file_path)
  # 2) clean 작업 : 이상치 제거 (100도 이하만 추출 -> boolean indexing)
  target_df = df[df['temperature'] <= 100].copy()
  # 3) 파생변수 생성 : 섭씨 -> 화씨 (섭씨*9/5+32)
  target_df["temperature_f"] = round((target_df["temperature"] * 9/5) + 32, 2)
  logging.info(f'가공 데이터 (rows, columns) {target_df.shape}')

  # 전처리된 내용 저장
  csv_file_path = f"{DATA_PATH}/preprocessing_sensor_data_{kwargs['ds_nodash']}.csv"
  target_df.to_csv(csv_file_path, index=False)
  logging.info(f'가공된 데이터 저장 {csv_file_path}')

  # 전달
  return csv_file_path

  
with DAG(
  dag_id            = "06_multi_dag_2_transform",
  description       = "Transform 전용 DAG",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = "@daily",
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['transform', 'ETL']
) as dag:
  task_transform      = PythonOperator(
    task_id = "transform",
    python_callable = _transform_cb
  )
  task_trigger_load_dag_run = TriggerDagRunOperator(
    task_id = "trigger_transform_dag_run",
    trigger_dag_id = "06_multi_dag_3_load",
    # 구동 시 전달시킬 데이터 -> XCom 통해 획득
    conf = {
      "csv_path" : "{{task_instance.xcom_pull(task_ids='transform')}}" 
    },
    # DAG 최초 수행시간 세팅 -> 첫번째 DAG과 동일하게 두번째 DAG도 해당 시간으로 최초 수행시간으로 간주할지
    reset_dag_run = True,
    # 기타 설정
    # 다음 DAG가 수행되는 것을 보고(대기) 종료할 것인가?
    wait_for_completion = False  # 명령 전달 후 바로 종료
  )

  task_transform >> task_trigger_load_dag_run
