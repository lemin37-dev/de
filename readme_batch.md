# 목표
- Silver layer -> Gold Layer로 데이터 파이프라인 연결
- 실시간 X, 배치작업 O
  - 매일 00시 5분 00초에 Airflow에 등록된 DAG가 동작해 집계 등의 처리를 통해 Gold에 필요한 형태로 가공
    - pandas/polars/spark를 활용
    - Athena(SQL)/opensearch(index search)/redshift(열검색 대용량 처리) 등 aws 서비스 활용

  
# 인프라 구성
- S3를 기존 로그제너레이터 버킷을 사용하도록 구성
- Glue와 Athena 생성하여 S3 데이터 조회/질의하도록 구성


# 인프라 구성후 데이터 확인
- 아테나 > 본인 작업 그룹 선택 > db 선택 > 쿼리 편집기 아래 쿼리 실행
```
-- 데이터의 총개수 조회
select count(*) as cnt
from silver_logs_tbl
where year = '2026'
	and month = '08'
	and day = '26'
	and hour = '21'
;
```

# 시나리오
- 데이터를 계속해서 데이터 파이프라인을 통해 저장하는 상황
- 1시간 기준으로 파티셔닝을 통해 분리되고 있음
- 미션
  - 8월 27일 00:05이 되면 Airflow에 등록된 DAG가 작동
  - 수집된 8월 26일 하루치 데이터를 Gold 데이터로 재구성
  - 방식
    - 과거 일별 집계 Gold가 모두 필요한지? -> 운영 모니터링
    - 전날 데이터만 필요한지? -> CTAS : Create Table As Select
  - 집계 데이터 샘플
  ```
        | domain    | event_type   | service          |   total | error | avg latency |
        | --------- | ------------ | ---------------- | ------: | ----: | ----------: |
        | ecommerce | product_view | product-service  | 530,000 | 1,200 |          42 |
        | ecommerce | purchase     | order-service    | 140,000 |   850 |         121 |
        | finance   | transfer     | transfer-service | 300,000 |   320 |          83 |
        | game      | login        | auth-service     | 410,000 | 1,100 |          51 |
  ```
