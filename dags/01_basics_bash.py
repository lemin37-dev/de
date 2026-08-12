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