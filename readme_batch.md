# 목표
- Silver layer -> Gold Layer로 데이터 파이프라인 연결
- 실시간 X, 배치작업 O
  - 매일 00시 5분 00초에 Airflow에 등록된 DAG가 동작해 집계 등의 처리를 통해 Gold에 필요한 형태로 가공
    - pandas/polars/spark를 활용
    - Athena(SQL)/opensearch(index search)/redshift(열검색 대용량 처리) 등 aws 서비스 활용

  
# 인프라 구성
- S3를 기존 로그제너레이터 버킷을 사용하도록 구성