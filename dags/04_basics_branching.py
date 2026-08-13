'''
- 파이썬 오퍼레이터의 콜백함수 내부연산의 결과로 조건부로 Task를 선택하여 진행
- 의존성 컨트롤, 조건부 Task 진행 -> Branch
- 의존성 구성에서 여러 시나리오 작성
'''
# 1. 모듈
from airflow import DAG
from airflow.operators.empty import EmptyOperator # 조건부 선택
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule  # 성공, 실패 등 조건 설정
from datetime import datetime, timedelta
import logging
import pendulum
import random

# 2. 전역변수
KST = pendulum.timezone("Asia/Seoul")

# 4-1. 콜백함수 정의
def _branch_cb(**kwargs):
  '''
    분기 처리 -> 특정 Task로 다음 수행 지정 -> task_id를 반환
  '''
  if random.choice([True, False]):
    logging.info("Task process 분기")
    return "process"  # 다음 수행할 task_id 값을 반환
  else:
    logging.info("Task skip 분기")
    return "skip"

def _process_cb(**kwargs):
  logging.info("Task process 수행")

# 3. DAG
with DAG(
  dag_id            = "04_basics_branching",
  description       = "분기처리, 선택적 Task 구동",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = "@daily",
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['branch', 'trigger_rule']
) as dag:
  # 4. Operator 정의
  task_start   = EmptyOperator(
    task_id = "start"
  )
  task_branch  = BranchPythonOperator(
    task_id = "branch",
    python_callable = _branch_cb
  )
  task_process = PythonOperator(
    task_id = "process",
    python_callable = _process_cb
  )
  task_skip    = EmptyOperator(
    task_id = "skip"
  )
  task_end     = EmptyOperator(
    task_id = "end",
    # Task 전체 수행에 대한 조건 부여
    # 분기 처리를 수행할 때에는 진행하지 않는 Task가 존재할 수 있기 때문에 성공/엔딩의 기준을 설정해야 함
    # NONE_FAILED_MIN_ONE_SUCCESS : 실패는 없고 최소 하나의 성공이 있을 때만
    trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
  )

  # 5. 의존성 정의 (시나리오 구성)
  task_start >> task_branch
  task_branch >> task_process >> task_end
  task_branch >> task_skip    >> task_end