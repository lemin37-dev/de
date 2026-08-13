'''
- airlfow 내부 정보 접근 시 macro활용 / 출력 시 jinja 활용
- 콜백 함수 내부에서는 kwargs 인자를 통해 접근, 기타 일반적인 상황에서는 jinja를 이용하여 접근
'''
# 1. 모듈
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pendulum

# 2. 전역변수
KST = pendulum.timezone("Asia/Seoul")

# 4-1. 콜백함수
def _print(**kwargs):
  logging.info(f'ds 출력 { kwargs["ds"] }')
  pass

# 3. DAG
with DAG(
  dag_id            = "03_basics_context_jinja",
  description       = "macro 이용하여 context 접근, jinja를 통해 표현",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = "0 9 * * *",  # cron 방식으로 표기 (매일 오전 9시)
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['macro', 'context', 'jinja']
) as dag:
  # 4. Operator 정의
  t1 = BashOperator(
    task_id = "jinja_used_task",
    bash_command = "echo 'DAG의 t1 수행시간 {{ ds }}, {{ ti }}'"
  )
  t2 = BashOperator(
    task_id = "jinja_macro_task",
    bash_command = "echo 'DAG의 t1 일주일 전 수행시간 {{ macros.ds_add(ds, -7) }}, 랜덤 숫자 {{ macros.random() }}'"
  )
  t3 = PythonOperator(
    task_id = "jinja_python_task",
    python_callable = _print
  )

  # 5. 의존성 정의
  t1 >> t2 >> t3