'''

'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime, timedelta
import pendulum

# 환경변수
BUCKET_NAME      = "de-ai-19-bk"
UPLOAD_FILE_NAME = "sensor_data.csv"
LOCAL_PATH       = f"/opt/airflow/dags/data/{UPLOAD_FILE_NAME}"
KST              = pendulum.timezone("Asia/Seoul")

# 콜백함수 정의
def _check_s3(**kwargs):
  pass

# DAG 정의
with DAG(
  dag_id            = "12_EMR_SPARK",
  description       = "대용량 데이터를 분산환경에서 처리하기 위해 스파크 사용",
  default_args      = {
                        "owner"           : "aic-de1-admin",  
                        "retries"         : 1,                    
                        "retry_delay"     : timedelta(minutes=1), 
                      },
  schedule_interval = "@daily",
  start_date        = pendulum.datetime(2026, 6, 29, tz=KST),
  catchup           = False,
  tags              = ['aws', 'spark', 'emr']
) as dag:
  # Task 정의
  create_cluster_task = EmrCreateJobFlowOperator()
  dummy_task = PythonOperator()
  run_spark_task = EmrAddStepsOperator()
  watch_spark_task = EmrStepSensor()
  terminate_cluster_task = EmrTerminateJobFlowOperator()

  # 의존성
  create_cluster_task >> dummy_task >> run_spark_task >> watch_spark_task >> terminate_cluster_task