'''
- 고객 정보가 담긴 데이터베이스가 있음
  CREATE TABLE IF NOT EXISTS customers (
      user_id VARCHAR(50) PRIMARY KEY,
      income INT DEFAULT NULL,
      loan_amt INT DEFAULT NULL,
      credit_score INT DEFAULT NULL,
      grade VARCHAR(10) DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
- 매일 고객이 가입, 업무 등의 DB 내용이 갱신됨
- 다음날 00시 01분 00초에 고객 DB 가져와(extract) 신용평가 진행(api-server) -> 고객정보 업데이트
- 배치 데이터 프로세스 작업
  - t1 : 테이블이 없으면 생성, 고객 데이터는 더미로 입력(매번 수행) -> 원래 배치 작업에서는 필요없는 작업임
  - t2 : 고객 데이터 획득 (DB -> DAG) -> XCom 게시 (df or dict)
  - t3 : XCom 데이터 획득 -> API 호출 -> 서버에서 고객데이터를 활용해 신용평가 진행 -> 응답받아 XCom에 게시
  - t4 : XCom 데이터 획득 -> 평가 결과를 고객 DB에 업데이트
'''
# 모듈 import
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import logging
import pendulum
import random
# API 호출용
import requests
# hash값 생성
import uuid

# 전역변수
KST = pendulum.timezone("Asia/Seoul")
API_URL = 'http://ai-api-server:8000/predict'

# 콜백함수 구성
def _create_dummy_data(**kwargs):
  '''
    테이블 생성(없으면), 더미 데이터 생성 및 삽입
  '''
  hooks = MySqlHook(mysql_conn_id="mysql_conn")
  try:
    with hooks.get_conn() as conn:
      logging.info(f'Connection success')
      
      with conn.cursor() as cursor:
        # Create table
        create_table_sql = '''
          CREATE TABLE IF NOT EXISTS customers (
              user_id VARCHAR(50) PRIMARY KEY,
              income INT DEFAULT NULL,
              loan_amt INT DEFAULT NULL,
              credit_score INT DEFAULT NULL,
              grade VARCHAR(10) DEFAULT NULL,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )
        '''
        cursor.execute(create_table_sql)

        # Insert customer dummy data
        insert_customer_sql = '''
          insert into customers
          (user_id, income, loan_amt)
          values
          (%s, %s, %s)
        '''
        params = [
          (
            f'u-{str(uuid.uuid4().hex)}', # user_id
            random.randint(3000, 10000),  # income
            random.randint(1000, 5000),   # loan_amt
          )
          for _ in range(50)
        ]
        cursor.executemany(insert_customer_sql, params)

        # commit
        conn.commit()
        pass
  except Exception as e:
    # 예외처리 수행 -> 오류로 확정하여 실패로 만들거나 분기를 해야함
    logging.error(f'SQL Error : {e}')
  else:
    logging.info(f'데이터베이스 처리 정상')  
  finally:
    logging.info(f'데이터베이스 작업 완료')
    pass

def _extract_user_data(**kwargs):
  '''
  - 신용평가 점수가 없는 고객들을 모두 가져와 XCom에 게시 ([dict, dict, ...])
  - sql을 수행하여 데이터 획득 과정
    - RDS 액세스
    - AWS Athena/Glue를 통해 SQL을 수행하여 S3에서 직접 획득
    - AWS Opensearch를 통해 검색 수행하여 직접 획득
    - ...
  '''
  hooks = MySqlHook(mysql_conn_id="mysql_conn")
  df = hooks.get_pandas_df('''
    select user_id, income, loan_amt
    from customers
    where credit_score is null;
  ''')
  if df.empty:
    logging.info("신규 고객 없음")
    return ""
  else:
    logging.info(f"평가 대상 {df.shape[0]}명")
    return df.to_dict(orient="records")
  
def _api_service_call(**kwargs):
  '''
  - XCom에서 데이터 획득 -> api 호출 -> 응답을 역직렬화 / 응답 실패 시 실패 raise -> XCom 게시
  '''
  target_user_data = kwargs["ti"].xcom_pull(task_ids="task_extract_user_data")
  logging.info(f'신용평가 대상 고객 {target_user_data}')

  # api 호출
  try:
    res = requests.post(API_URL, json=target_user_data) # json 문자열 형태로 데이터 전달
    results = res.json()  # 역직렬화 (str -> list(dict))
    logging.info(f'평가 결과 {results}')
    return results
  except Exception as e:
    logging.error(f'통신 오류 {e}')
    raise

def _load_user_credit(**kwargs):
    eval_user_data = kwargs["ti"].xcom_pull(task_ids="task_api_service_call")
    if not eval_user_data:
      logging.error("신용평가 결과 없음")
      raise ValueError("신용평가 결과 없음")

    hooks = MySqlHook(mysql_conn_id="mysql_conn")
    with hooks.get_conn() as conn:
      with conn.cursor() as cursor:
        update_customer_sql = '''
          update customers
          set credit_score=%s, grade=%s
          where user_id = %s
        '''
        params = [
          (data['credit_score'], data['grade'], data['user_id'])
          for data in eval_user_data  # -> XCom으로 전달된 데이터는 타입유지
        ]
        cursor.executemany(update_customer_sql, params)
        conn.commit()
    pass

# DAG 정의
with DAG(
  dag_id            = "07_api_server_used",
  description       = "특정 주기 단위로 고객 DB의 신용평가 정보 업데이트",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = "@daily",
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['api', 'ETL']
) as dag:
  # Task 정의
  t1       = PythonOperator(
    task_id = "create_dummy_data",
    python_callable = _create_dummy_data
  )
  t2      = PythonOperator(
    task_id = "task_extract_user_data",
    python_callable = _extract_user_data
  )
  t3    = PythonOperator(
    task_id = "task_api_service_call",
    python_callable = _api_service_call
  )
  t4         = PythonOperator(
    task_id = "task_load_user_credit",
    python_callable = _load_user_credit
  )

  # 의존성
  t1 >> t2 >> t3 >> t4