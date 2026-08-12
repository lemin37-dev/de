# 데이터 생명주기
```
┌──────────────────────────────────────────────────────────────┐
│                 DATA ENGINEERING LIFECYCLE                   │
└──────────────────────────────────────────────────────────────┘

1. DATA GENERATION  : Log/Event/생성/Action/DB (dummy, service, sensor)
   데이터 발생
        │
        ▼
2. DATA INGESTION   : firehose > kinesis/kafka, E`L`K, E`F`K, ... 서비스
   데이터 수집 / 유입
        │
        ▼
3. DATA STORAGE     : S3(Data Lake) 등 ...
   원본 저장
        │
        ▼
4. DATA PROCESSING  : ETL. ELT, 메달리온 아키텍쳐, S3 | pandas/polars/spark (데이터 규모에 따라 결정)
   정제 / 변환 / 집계
        │
        ▼
5. DATA MODELING    : 대시보드 용 관제, 인사이트 도출, 모델학습, ... -> athena, opensearch, ...
   분석 가능한 데이터 구조화
        │
        ▼
6. DATA SERVING     : 실제 서비스
   조회 / 분석 / 활용

────────────────────────────────────────────────────────────
전체 과정에 걸쳐

7. Orchestration    : 오케스트레이션, Airflow(배치 프로세싱) -> 데이터 파이프라인 구축 (도메인에 따라 구성 상이)
8. Observability    : 그라파나/프로메테우스/ELK에서는 키바나, opensearch 대시보드 등 활용
```

# DATA GENERATION 
- 데이터가 어디서 발생하는가? 
  - 목표
    - 데이터를 더미로 발생(클라이언트가 서비스를 통해 생성한 데이터를 가정해 정상/비정상 데이터 비율 산정) -> S3 적재
    - 더미 구현을 위해 python, aws ecs + fargate 로 구성
  - 도메인결정 -> 실제 데이터 샘플 획득, 불량율 획득 -> 구성 (프로젝트)
  - 형태
    - [v] 최종 형태를 더미로 구성하여 적재
      - 파이썬 코드 작성(faker 패키지 활용)
    - 실제 요청 구성 -> 서비스 통과 -> 결과 -> 적재
      - 최종 프로젝트 형태
  - 방식
    - [v] ECS Fargate + K6(..) -> 클라이언트의 요청 구현
      - 미니 프로젝트 구성
    - AWS Distributed Load Testing -> 대규모 트레픽 발생

- 도메인  
  - 어플리케이션 (웹, 앱)
  - IOT 센서
  - 게임 서버
  - 금융 시스템
  - 데이터베이스
  - ...

- 데이터 발생 (요청 -> 응답)
```
┌──────────────────────────────────────────────────────────────────┐
│                    TRAFFIC GENERATOR                             │
└──────────────────────────────────────────────────────────────────┘

                       ECS Fargate
                            │
                            ▼
                           k6
                            │
                            │ HTTP / HTTPS Request
                            ▼
                       Public ALB
                            │
                            ▼
                    Dummy Service
                      FastAPI
                            │
             ┌──────────────┼──────────────┐
             │              │              │
             ▼              ▼              ▼
        GET Request    POST Request    Error Request
             │              │              │
             └──────────────┼──────────────┘
                            ▼
                       Response
                            │
                            ▼
                           k6
```

- 데이터 트래픽 -> 적재 형태
```
┌──────────────────────────────────────────────────────────────────┐
│                    LOG INGESTION                                 │
└──────────────────────────────────────────────────────────────────┘

                     HTTP Request
                          │
                          ▼
                    Dummy Service
                      FastAPI
                          │
                          │
                   Log / Event 생성
                          │
          ┌───────────────┼────────────────┐
          │               │                │
          ▼               ▼                ▼
      Access Log     Business Event     Error Log
          │               │                │
          └───────────────┼────────────────┘
                          │
                          ▼
                   CloudWatch Logs
                          │
                          ▼
                 Amazon Data Firehose   : 1분 단위, 100mb 단위 등의 기준으로 데이터를 모아서 전달
                          │
                          ▼
                      S3 BRONZE         : 적재 위치 (Bronze : 원본 데이터 형태)
                  ─────────────────
                       Raw Log
                       Raw Event
                       JSON / GZIP
                       Partition
```

- S3 저장 샘플링 - 검색을 위해 파티션작업(검색속도 향상), 저장형태(`파케이`, 압축, ...)
    - S3, Athena(Serverless 형태로 raw 데이터에 sql 수행처리 가능)
    ```
    S3
    └─ bronze/
    ├─ access/
    │   └─ year=2026/month=08/day=11/
    │
    ├─ event/
    │   └─ year=2026/month=08/day=11/
    │
    └─ error/
        └─ year=2026/month=08/day=11/
    ```

- 전체 구성 확장
```
┌──────────────────────────────────────────────────────────────────────┐
│                       LOG GENERATOR SYSTEM                           │
└──────────────────────────────────────────────────────────────────────┘


                     [ Traffic Generator ]

                        ECS Fargate
                             │
                             ▼
                            k6
                             │
                  HTTP / HTTPS Request
                             │
                             ▼
                        Public ALB
                             │
                             ▼
                       Dummy Service
                         FastAPI
                             │
                 ┌───────────┼───────────┐
                 │           │           │
                 ▼           ▼           ▼
              Access      Business     Error
               Log         Event        Log
                 │           │           │
                 └───────────┼───────────┘
                             │
                             ▼
                      CloudWatch Logs
                             │
                             ▼
                    Amazon Data Firehose
                             │
                             ▼

┌──────────────────────────────────────────────────────────────────────┐
│                         DATA LAKE                                    │
└──────────────────────────────────────────────────────────────────────┘

                          S3 BRONZE
                    ───────────────────

                     Raw Access Log
                     Raw Event Log
                     Raw Error Log
                     JSON / GZIP
                     Date Partition

                             │
                             │
                             ▼

                    다음 Pipeline Chapter

                             │
              ┌──────────────┼───────────────┐
              ▼              ▼               ▼
           Airflow       Kafka/MSK        Kinesis
           Batch         Streaming       Streaming
              │
              ▼
       Pandas / Polars / Spark
              │
              ▼
          S3 SILVER
              │
              ▼
           S3 GOLD
```

# DATA INGESTION (발생한 데이터를 어떻게 가져오는가?)
- 수집 방식에 따라 파이프라인 유형 (최초 발생 -> 저장)
```
                    DATA INGESTION

                         Source
                           │
        ┌──────────────────┼───────────────────┐────────────┐
        │                  │                   │            │
        ▼                  ▼                   ▼            ▼
 [V]Log / Event      [V]Streaming             CDC        [V]Batch 
 Ingestion             Ingestion           Ingestion     Ingestion
        │                  │                   │            │
        ▼                  ▼                   ▼            ▼
 CloudWatch      Kafka / MSK / Kinesis    DMS / Debezium  CSV/API/DB Export (Ariflow/MWAA)
 Firehose
        │                  │                   │            │
        └──────────────────┼───────────────────┘────────────┘
                           ▼
                       S3 BRONZE
```

- Log / Event ingestion pipeline
    - 로그, 이벤트를 수집하여 저장
    - 실시간 분석 X, 데이터를 안전하게 쌓는 것이 목표 -> 사용시간은 상이함(즉시성은 아님)
    ```
    ┌─────────────────────────────────────────────────────┐
    │          01. LOG / EVENT INGESTION PIPELINE         │
    └─────────────────────────────────────────────────────┘

                    로그제너레이터
                        │
                        ▼
                CloudWatch Logs
                        │
                        ▼
                Amazon Data Firehose
                        │
                        ▼
                    S3 BRONZE
                        │
                    Raw Data 저장
    ```

- Batch processing ingestion pipeline
    - 쌓여있는 데이터를 일정 주기로 처리 -> Airflow
    - Airflow/MWAA <-> Step Functions + lambda : 서버리스(비용절감)
    - 스케줄링 (규칙적인 시간에 작업해야 하는 경우)
    ```
    ┌─────────────────────────────────────────────────────┐
    │             02. BATCH DATA PIPELINE                 │
    └─────────────────────────────────────────────────────┘

                    S3 BRONZE
                        │
                        ▼
                    Airflow : 스케줄 작성 -> DAG 작성 -> 데이터를 전처리
                        │
            ┌───────────┼───────────┐
            ▼           ▼           ▼
        Pandas(소)  Polars(중)   Spark(대)  <-- 데이터 처리 규모에 따라 분류
            │           │           │
            └───────────┼───────────┘
                        ▼
                    S3 SILVER
                        │
                        ▼
                    S3 GOLD
                        │
                        ▼
                Athena / Redshift
    ```


- Real-time Stream ingestion pipeline
    - 데이터가 들어오는 동시에 즉시 처리
    - 공장 설비/생산 시 이상 탐지, 금융 이상거래 감지, 게임, 실시간 주문량, ...
    - Kafka/ MSK, Kinesis, (ELK, EFK, opensearch) : 데이터 전송
    - Flink : 실시간 데이터를 가공
    ```
    ┌─────────────────────────────────────────────────────┐
    │          03. REAL-TIME STREAMING PIPELINE           │
    └─────────────────────────────────────────────────────┘

                    로그제너레이터
                        │
            ┌───────────┴────────────┐
            ▼                        ▼
    [V] Kafka / MSK          [V] Kinesis Data Streams
            │                        │
            └───────────┬────────────┘
                        ▼
                    Spark Streaming
                        또는
                    [V] Flink
                        │
                Window / Aggregate
                Filter / Detection
                        │
            ┌───────────┼────────────┐
            ▼           ▼            ▼
    [V]OpenSearch   S3 Silver   Alert/Event
      (ELK, EFK)  
    ```


# 중간 정리 : 구조 유형
- Application log
    - 앱, 웹, IOT, 팩토리, 금융, 게임
- Business Event
    - 행위 (로그인, 검색, 스크롤, ...)
- Database Change
    - Update, Delete, ...



# DATA STORAGE
- 수집한 데이터를 어디에 저장하는가?
    - 원본을 잃지 않고 저장
    - 수집 시 장애 등의 문제로 누락 -> 백필 작업 (누락분 다시 수집)
- 기간
    - 일정 기간 (통상 7일, 세부 조정가능)
        - kafka, kinesis
    - 영구 보관
        - 데이터 레이크 -> S3 (객체로 저장, 버킷단위로 관리, 검색지원)
- 데이터 저장 형태 변화 예시
    - csv -> json -> parquet(분석용, 열단위)
- 흐름
    ```
        Ingestion
            │
            ▼
        S3 BRONZE
    ───────────────
    Raw JSON  => *.json, *.json.gz
    Raw CSV
    Raw Event
    Raw Log
    CDC Data
    ------------------- => *.parquet
    ```
- 파일 포맷 형태 및 사용용도

    | 분류            | 대표 확장자                                    | 주요 용도                   |
    | ------------- | ----------------------------------------- | ----------------------- |
    | **정형 데이터**    | `.csv`, `.tsv`, `.xlsx`                   | 매출, 고객, 주문 등 테이블 데이터    |
    | **반정형 데이터**   | `.json`, `.jsonl`, `.xml`, `.yaml`        | API 응답, 이벤트, 로그         |
    | **분석용 컬럼 포맷** | `.parquet`, `.orc`                        | Athena, Spark, Glue 분석  |
    | **빅데이터 포맷**   | `.avro`                                   | Kafka, Spark, 데이터 파이프라인 |
    | **로그 파일**     | `.log`, `.txt`, `.json`, `.json.gz`       | 웹/API/애플리케이션 로그         |
    | **압축 파일**     | `.gz`, `.zip`, `.tar.gz`, `.bz2`          | 저장 공간/전송량 절감            |
    | **이미지**       | `.jpg`, `.png`, `.webp`, `.gif`, `.svg`   | 상품 이미지, ML 데이터          |
    | **영상**        | `.mp4`, `.mov`, `.avi`, `.mkv`            | CCTV, 콘텐츠, ML 데이터       |
    | **오디오**       | `.mp3`, `.wav`, `.flac`                   | 음성 데이터, AI 학습           |
    | **문서**        | `.pdf`, `.docx`, `.pptx`, `.hwp`          | 문서 보관                   |
    | **DB 백업**     | `.sql`, `.dump`, `.bak`                   | 데이터베이스 백업               |
    | **ML/AI 모델**  | `.pkl`, `.joblib`, `.pt`, `.pth`, `.onnx` | 모델 아티팩트                 |
    | **웹 파일**      | `.html`, `.css`, `.js`                    | 정적 웹사이트                 |
    | **바이너리**      | `.bin`, `.dat` 등                          | 기타 애플리케이션 데이터           |

- 저장 형태 (메달리온 아키텍처 기준) + 파티션 적용
    ```
    S3
    │
   버킷 (계정/프로젝트/팀별 기준 적용)
    │
    │
    ├── bronze/      # Raw data
    │   ├── api_logs/
    │   │   └── *.json.gz
    │   ├── clickstream/
    │   │   └── *.json
    │   └── orders/
    │       └── *.csv
    │
    ├── silver/      # 클리닝, 전처리 등 1차가공된 데이터 (열기준)
    │   ├── orders/
    │   │   └── *.parquet
    │   └── customers/
    │       └── *.parquet
    │
    └── gold/       # 최종 데이터 형태, 데이터파이프라인의 종착점에 필요한 데이터 형태
        ├── daily_sales/
        │   └── *.parquet
        └── customer_summary/
            └── *.parquet
    ```















































- ETL 파이프라인
    - Extract-Transform-Load, 데이터 엔지니어의 전통적인 유형
    - 메달리온 아키텍쳐 구조에서 사용 -> 각 레이어에서 데이터 형태 설명
    ```
    ┌─────────────────────────────────────────────────────┐
    │                 04. ETL PIPELINE                    │
    └─────────────────────────────────────────────────────┘

                Source / S3 BRONZE
                        │
                     Extract
                        │
                        ▼
                  Pandas / Polars
                  Spark / Glue (ETL, 데이터베이스, 테이블, 크롤러 등 활용)
                        │
                    Transform
                        │
                        ▼
                      Load
                        │
                        ▼
                    S3 SILVER
                        │
                        ▼
                     S3 GOLD
    ```

- ELT 파이프라인
    - 금융/제조 프로젝트 활용, 저장 후 SQL 변환 처리
    ```
    ┌─────────────────────────────────────────────────────┐
    │                 05. ELT PIPELINE                    │
    └─────────────────────────────────────────────────────┘

                    Data Source
                        │
                     Extract
                        │
                        ▼
                      Load
                        │
                        ▼
                  S3 / Redshift
                        │
                        ▼
                  SQL Transform
                        │
                        ▼
                  Silver / Gold
                        │
                        ▼
                  Analytics Mart
    ```