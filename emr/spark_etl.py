# DAG에 의해 작동

# 모듈 가져오기
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys

# 인자값 추출 -> Airflow에서 수행시간 정보 전달
if len(sys.argv) > 1:
  TARGET_DATE = sys.argv[1]
else:
  raise ValueError("날짜 인자가 누락되었습니다. (YYYY-MM-DD)")

# 버킷정보
BUCKET_NAME = "de-ai-19-bk"
INPUT_PATH  = f"s3://{BUCKET_NAME}/spark/bronze/raw_data.json"
OUTPUT_PATH = f"s3://{BUCKET_NAME}/spark/silver/"

# Sparkf를 통한 ETL 처리함수 정의
# 컨셉 : 브론즈 -> 실버 (정제 + 처리시간 기록)
def clean_processing():
    # spark session 생성
    spark = (SparkSession
             .builder
             .appName(f'Daily_Data_Cleaning_{TARGET_DATE}')
             .getOrCreate()
    )

    # [Extract] 데이터 추출, 스키마 준비
    # {"event_id": "301fd6ae-6f70-4651-9575-2a8884b56a62", "user_id": "user_24", "event_type": "error", "product_id": 1531, "price": 91433, "timestamp": "2026-09-15 21:15:42", "os": "Windows"}
    schema = StructType(
       StructField('event_id', StringType(), True),
       StructField('user_id', StringType(), True),
       StructField('event_type', StringType(), True),
       StructField('product_id', IntegerType(), True),
       StructField('price', IntegerType(), True),
       StructField('timestamp', StringType(), True),
       StructField('os', StringType(), True),
    )
    # lazy load
    raw_df = spark.read.schema(schema).json(INPUT_PATH)
    # 원본 데이터 확인
    print(f'원본 데이터 개수 {raw_df.count()}')

    # [Transform] 정제(필터링)
    clean_df = (
       raw_df
       .filter(F.col('user_id').isNotNull())
       .filter(F.col('price') > 0)
      #.filter(F.col('timestamp').try_cast(DateType()).isNotNull())
       .withColumn('event_time', F.to_timestamp(F.col('timestamp'), "yyyy-MM-dd HH:mm:ss"))
       .filter(F.col('event_time').isNotNull())
       .fillna({"event_type":"unknown"})
       .dropDuplicates(['event_id'])
    )

    # [Transform] 파생변수(처리시간) 생성
    final_df = clean_df.withColumn('processed_at', F.current_timestamp())
    print(f'전처리 후 데이터 개수 {final_df.count()}')

    # [Load] 적재 (parquet)
    final_df.write.mode('overwrite').parquet(OUTPUT_PATH)

    # spark session close
    spark.stop()
    pass


# entry point
if __name__=="__main__":
   clean_processing()


