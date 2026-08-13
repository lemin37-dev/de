'''
- ETL 간단하게 적용, 데이터 등 더미 구성, 적재 위치는 mysql 
- 데이터 소규모 -> pandas 사용
- 1개의 DAG에서 ETL 처리
- 필요 패키지 : 로컬 PC 기반 apache-airflow-providers-mysql pandas
  pip install apache-airflow-providers-mysql pandas
'''
# 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
# 범용 SQL 처리 오퍼레이터
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Load 처리 시 데이터 밀어 넣기 시 활용
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import logging
import pendulum
# 데이터
import json
import random
import pandas as pd
import os

# 전역변수
KST = pendulum.timezone("Asia/Seoul")
DATA_PATH = "/opt/airflow/dags/data"
os.makedirs(DATA_PATH, exist_ok = True)

# 콜백 함수
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
  
def _transform_cb(**kwargs):
  # 앞선 Task가 전달한 내용을 XCom을 통해 획득
  ti = kwargs["ti"]
  json_file_path = ti.xcom_pull(task_ids="extract")
  logging.info(f'전달받은 데이터 파일 경로 {json_file_path}')

  # transform -> 데이터 clean, 전처리(단위 변경, 파생변수, ...)
  # 섭씨 온도를 화씨 온도 계산 -> 파생변수 추가 -> pandas의 DataFrame 활용
  # 섭씨 온도 100도 이하만 센서가 정상, 그 이상은 이상탐지의 대상으로 간주 -> 이상치 제거
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

def _load_cb(**kwargs):
  # csv 경로 획득
  ti = kwargs["ti"]
  csv_file_path = ti.xcom_pull(task_ids="transform")

  # csv -> df 로드
  df = pd.read_csv(csv_file_path)

  # mysql 연결 -> 데이터 삽입
  hooks = MySqlHook(mysql_conn_id="mysql_conn")
  try:
    with hooks.get_conn() as conn:
      logging.info(f'Connection success')
      # 커서 획득
      with conn.cursor() as cursor:
        # insert 구문 작성
        sql = '''
          insert into sensor_readings
          (sensor_id, timestamp, temperature_c, temperature_f)
          values
          (%s, %s, %s, %s)
        '''
        # param 구성
        params = [
          (data['sensor_id'], data['timestamp'], data['temperature'], data['temperature_f'])
          for _, data in df.iterrows() # (인덱스, 데이터) 형태로 반환됨
        ]
        # 쿼리 실행
        cursor.executemany(sql, params)
        # commit
        conn.commit()
        pass
  except Exception as e:
    logging.error(f'SQL Error : {e}')
  else:
    logging.info(f'데이터베이스 처리 정상')  
  finally:
    logging.info(f'데이터베이스 작업 완료')
    pass

# DAG 정의
with DAG(
  dag_id            = "05_mysql_etl",
  description       = "ETL 수행하여 mysql에 온도 센서 데이터 적재",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = "@daily",
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['mysql', 'ETL']
) as dag:
  # Task 정의
  task_create_table = SQLExecuteQueryOperator(
    task_id = "create_table",
    # 접속정보 설정
    conn_id = "mysql_conn",
    sql = '''
      CREATE TABLE IF NOT EXISTS sensor_readings (
          id INT AUTO_INCREMENT PRIMARY KEY,
          sensor_id VARCHAR(50),
          timestamp DATETIME,
          temperature_c FLOAT,
          temperature_f FLOAT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    '''
  )
  task_extract      = PythonOperator(
    task_id = "extract",
    python_callable = _extract_cb
  )
  task_transform    = PythonOperator(
    task_id = "transform",
    python_callable = _transform_cb
  )
  task_load         = PythonOperator(
    task_id = "load",
    python_callable = _load_cb
  )

  # 의존성
  task_create_table >> task_extract >> task_transform >> task_load
