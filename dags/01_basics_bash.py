'''
- 기본 DAG 연습
- DAG의 기본 형태가 갖춰지지 않으면 대시보드에 등록되지 않음(작업으로 등록되지 않음)
- 필수 구성만 갖추면 대시보드 상에 작업으로 등록됨

- 목표
  - bash operator 테스트, DAG 인식, DAG 작동확인, DAG 기본 구성

- 작동확인
  - 대시보드(시각화)
  - 로그

- host pc에서 작성한 해당 파일은 xxx-workcer 컨테이너에 /opt/airflow/dags 하위에 동기화됨
- 실제는 xxx-worker 컨테이너에서 가동됨
'''

# 1. 필요한 모듈, 패키지 가져오기
# DAG 클래스
from airflow import DAG
# 오퍼레이터 2.x (3.x 에서는 패키지 경로가 변경됨)
from airflow.operators.bash import BashOperator
# 스케줄 -> 시간처리
from datetime import datetime, timedelta

# 2. DAG 정의 -> DAG 세션이 오픈
with DAG(
  dag_id = "01_basics_bash"  # DAG 간 구분하는 용도
) as dag:
  # 3. Operator 정의

  # 4. 의존성, 구동순서 정의
  pass