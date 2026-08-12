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
# 2-1. DAG에서 활용할 default_args 정의
# 시나리오
# 작업 성공 -> 완료
# 작업 실패 -> 5분 대기 -> 1회 재시도 -> 성공 -> 완료
# 작업 실패 -> 5분 대기 -> 1회 재시도 -> 실패 -> 완료(실패)
# 향후 작업이 재개되어도 누락된 과거 데이터 소급(Backfill) X
default_args = {
  "owner"           : "aic-de1-admin",      # DAG 소유자
  "depends_on_past" : False,                # 과거 데이터(가동시간 대비) 소급 처리여부
  "retries"         : 1,                    # 작업 실패 시 재시도 횟수
  "retry_delay"     : timedelta(minutes=5), # 작업 실패 후 재시도 delay 정의
}

with DAG(
  dag_id            = "01_basics_bash",  # DAG 간 구분하는 용도
  description       = "DE 업무 중 배치 파이프라인 구성 중 데이터 프로세싱 오케스트레이션 담당 airflow의 DAG 작성 기본형",  # DAG 설명
  default_args      = default_args, # 기본 인자값
  schedule_interval = "@daily",  # 하루 1회 00시 00분 00초, 문자열, cron 표현
  start_date        = datetime(2026,6,29),  # 현재 기준 갭이 발생해도 "depends_on_past" 값을 false로 설정해 소급적용 X
  catchup           = False,   # 과거에 대한 소급 처리 실행 방지
  tags              = ['bash', 'basic']   # DAG 검색(특정)을 위해 자유롭게 세팅
) as dag:
  # 3. Operator 정의
  t1 = BashOperator(  # Task 정의
    task_id = "data-print",  # 영문, 숫자, 하이픈(-), 마침표(.) 언더바(_) 사용가능
    bash_command = ""
  ) 
  t2 = BashOperator(
    task_id = "sleep",
    bash_command = ""
  )
  t3 = BashOperator(
    task_id = "echo-print",
    bash_command = ""
  )

  # 4. 의존성, 구동순서 정의
  # t1 - t2 - t3 순으로 실행 (성공이 전제)
  t1 >> t2 >> t3
  pass