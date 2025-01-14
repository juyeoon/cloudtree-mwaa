import boto3
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def read_sql_from_s3(bucket_name, key):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read().decode("utf-8")


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
        job_name="cultural_event_info_job",  # 실행할 Glue Job 이름
    )
    # S3에서 SQL 파일 읽기
    fetch_query = PythonOperator(
        task_id="fetch_sql_from_s3",
        python_callable=read_sql_from_s3,
        op_kwargs={
            "bucket_name": "pjy-mwaa-athena-output",
            "key": "inputQuery/MSCK_cultural_event_info.sql",
        },
    )
    # AthenaOperator를 사용하여 쿼리 실행
    run_athena_query = AthenaOperator(
        task_id="run_query",
        query="{{ ti.xcom_pull(task_ids='fetch_sql_from_s3') }}",  # XCom에서 쿼리 내용 가져오기
        database="cloudtree_transformed_db",
        output_location="s3://cloudtree-athena-query-result/mwaa-dag-query-results/",
        # aws_conn_id="aws_default",
    )

    run_glue_job >> fetch_query >> run_athena_query
