'''

'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime, timedelta
import pendulum

# 환경변수
BUCKET_NAME       = "de-ai-19-bk"
SPARK_SCRIPT_PATH = f"s3://{BUCKET_NAME}/spark/scripts/spark_etl.py"
EMR_LOG_URI       = f"s3://{BUCKET_NAME}/spark/emr_logs/"
KST               = pendulum.timezone("Asia/Seoul")

# 인프라 설정
JOB_FLOW_OVERRIDES = {
    "Name": "Airflow-Automated-EMR-Cluster-de19",
    # EMR 소프트웨어 버전 지정
    "ReleaseLabel": "emr-6.10.0",
    # Hadoop, Spark 설치
    # Hadoop : 스파크 구동에서 기반 역할
    "Applications": [
        {"Name": "Hadoop"},
        {"Name": "Spark"},
    ],
    # 인스턴스 생성(EC2 기반의 EMR)
    "Instances": {
        "Ec2SubnetId": "subnet-0928223142a64ef05",
        "InstanceGroups": [
            { # Master : EMR 클러스터 관리(스파크 작업/리소스/워커 관리) 노드
                "Name": "Master node",
                "Market": "SPOT", # <-> ON_DEMAND : 안정적, 비용 비교적 많이 듬
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            { # Worker
                "Name": "Core nodes",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        # 스파크 작업이 끝났다고 스스로 종료하는 것을 방지하는 옵션 (추후 Task를 통해 명시적으로 종료하기 위해서 True로 설정)
        "KeepJobFlowAliveWhenNoSteps": True,
        # 종료 보호 옵션 (True로 설정하면 추후 Task에서 종료할 수 없기 때문에 False로 설정)
        "TerminationProtected": False,
    },
    # EC2 IAM Role
    "JobFlowRole": "EMR_EC2_DefaultRole",
    # EMR Service 최소 권한을 가진 IAM Role
    "ServiceRole": "EMR_DefaultRole",
    # Log 경로 (S3)
    "LogUri": EMR_LOG_URI,
    "VisibleToAllUsers": True,
}
SPARK_SUBMITS = [
    {
        "Name": "Daily Data Cleaning Job",
        "ActionOnFailure": "CONTINUE",      # 실패해도 다음 스텝으로 진행
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster", # Spark 구동환경 (cluster)
                SPARK_SCRIPT_PATH,
                "2026-09-16" # 임시 편성
            ],
        },
    }
]

# 콜백함수 정의
def _dummy_task_cb(**kwargs):
  print('클러스터 생성 완료')
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
  create_cluster_task = EmrCreateJobFlowOperator( # EMR 클러스터 생성 (인프라 구성) -> 구성 후 클러스터를 참조할 수 있는 리소스 ID 자동반환
    task_id            = "create_cluster",
    job_flow_overrides = JOB_FLOW_OVVERRIDES,
    aws_conn_id        = "aws_default",
  )
  dummy_task = PythonOperator(  # 인프라 구성완료 확인 (생략가능)
    task_id         = "dummy",
    python_callable = _dummy_task_cb
  )
  run_spark_task = EmrAddStepsOperator( # 스파트 코드 작동
    task_id     = "run_spark",
    # Spark 작동환경(클러스터 ID 세팅)
    job_flow_id = "{{task_instance.xcom_pull(task_ids='create_cluster', key='return_value')}}",
    # Spark 지정 (spark_submut 실행)
    steps       = SPARK_SUBMITS,
    aws_conn_id = "aws_default",
  )
  watch_spark_task = EmrStepSensor( # 센서를 통해 스파크 작업 완료여부 확인
    task_id     = "watch_spark",
    # 감지할 Cluster 지정
    job_flow_id = "{{task_instance.xcom_pull(task_ids='create_cluster', key='return_value')}}",
    # Spark 작업 완료여부 체크
    step_id     = "{{task_instance.xcom_pull(task_ids='run_spark', key='return_value')[0]}}",
    aws_conn_id = "aws_default",
  )
  terminate_cluster_task = EmrTerminateJobFlowOperator( # EMR 클러스터 해제
    task_id      = "terminate_cluster",
    job_flow_id  = "{{task_instance.xcom_pull(task_ids='create_cluster', key='return_value')}}",
    aws_conn_id  = "aws_default",
    # 위의 Task들이 실패하더라도 반드시 EMR 삭제
    trigger_rule = "all_done"
  )

  # 의존성
  create_cluster_task >> dummy_task >> run_spark_task >> watch_spark_task >> terminate_cluster_task