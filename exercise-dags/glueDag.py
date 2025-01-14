from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

"""
# glue job을 실행하는 DAG
"""

# DAG 정의
with DAG(
    dag_id="simple_glue_job",
    start_date=datetime(2023, 1, 1),  # DAG 시작 날짜
    schedule_interval=None,  # 비정기 실행
    catchup=False,  # 이전 실행 무시
) as dag:
    # Glue Job 실행
    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="mwaa-test-job",  # 실행할 Glue Job 이름
        # script_location="s3://sample-bucket/mwaa-test-job.py",  # 스크립트 경로
        # script_args={"--S3_OUTPUT_PATH": "s3://sample-bucket"},  # Glue 작업에 전달할 파라미터
    )
