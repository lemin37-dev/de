'''
- PythonOperator 사용 패턴
- Task 간 통신 -> XCom 사용 (airflow 내부 컨텐스트 공간을 접근) -> Task간 상호 통신
- 공간의 한계 -> 공유 데이터는 raw 데이터가 아닌 raw 데이터에게 접근 가능한 메타데이터 제공
'''

# 1. 모듈 가져오기
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# 2. DAG 정의
# 2-1. 콜백함수 정의
def _extract_cb(**kwargs):
  '''
  - kwargs : airflow 작업하기 전에 내부 정보(context)를 접근할 수 있는 entrypoint
  - context : airflow 가 주입한 정보(Task 정보, 시간과 같은 meta정보), Task 전달 데이터
  '''
  # 1) 컨텍스트(airflow 내부 정보 저장공간)에서 특정 정보 추출
  ti        = kwargs["ti"]        # ti : Task Instance (Task Instacne 객체 획득)
  ds        = kwargs["ds"]        # ds : Task 수행 시간
  ds_nodash = kwargs["ds_nodash"] # ds_nodash : Task 수행 시간(dash 제거)
  run_id    = kwargs["run_id"]    # run_id : Task 실행 ID

  # 2) 로깅
  logging.info("=== Extract 작업 ===")
  logging.info(f"ti        = {ti}")
  logging.info(f"ds        = {ds}")
  logging.info(f"ds_nodash = {ds_nodash}")
  logging.info(f"run_id    = {run_id}")
  logging.info("===================")

  # 3) 정보 전달
  # return을 통해 xcom으로 특정 데이터를 push
  return f"ds = {ds} ds_nodash = {ds_nodash} run_id = {run_id}"


def _transform_cb(**kwargs):
  pass

# 2-2. DAG metadata 작성
with DAG(
  dag_id            = "02_basics_python",
  description       = "파이썬 Task, XCom 사용",
  default_args      = {
    "owner"           : "aic-de1-admin",  
    "retries"         : 1,                    
    "retry_delay"     : timedelta(minutes=1), 
  },
  schedule_interval = "@once",  # 수동으로 한번 수행, 주기성 X
  start_date        = datetime(2026,6,29),
  catchup           = False,
  tags              = ['python', 'xcom']
) as dag:
  # 3. Operator 정의  (ETL 중 ET 표현)
  extract_task   = PythonOperator(
    task_id         = "extract_task",
    python_callable = _extract_cb # 콜백함수
  )
  transform_task = PythonOperator(
    task_id         = "transform_task",
    python_callable = _transform_cb
  )

  # 4. 의존성 정의
  extract_task >> transform_task

  
