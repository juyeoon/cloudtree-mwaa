import boto3
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

"""
# S3에 있는 sql을 읽어 athena에서 쿼리를 수행하는 코드
"""


def read_sql_from_s3(bucket_name, key):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read().decode("utf-8")


# DAG 정의
with DAG(
    dag_id="simple_athena_query",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    # S3에서 SQL 파일 읽기
    fetch_query = PythonOperator(
        task_id="fetch_sql_from_s3",
        python_callable=read_sql_from_s3,
        op_kwargs={
            "bucket_name": "pjy-mwaa-athena-output",
            "key": "inputQuery/testquery.sql",
        },
    )
    # AthenaOperator를 사용하여 쿼리 실행
    run_athena_query = AthenaOperator(
        task_id="run_query",
        query="{{ ti.xcom_pull(task_ids='fetch_sql_from_s3') }}",  # XCom에서 쿼리 내용 가져오기
        database="cloudtree_raw_db",
        output_location="s3://pjy-mwaa-athena-output/athena-results/",
    )

    fetch_query >> run_athena_query
