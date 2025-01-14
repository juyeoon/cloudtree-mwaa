from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

"""
# AWS Lambda를 호출하는 DAG. ChatGPT 작성
"""

# 기본 설정 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['example@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 생성
with DAG(
    dag_id='invoke_lambda_dag',  # DAG 고유 식별자
    default_args=default_args,  # 기본 설정
    description='A DAG to invoke AWS Lambda functions using MWAA',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['example', 'aws', 'lambda'],
) as dag:

    # Lambda 함수를 호출하는 작업 정의
    invoke_lambda = AwsLambdaInvokeFunctionOperator(
        task_id='invoke_lambda',
        function_name='your_lambda_function_name',  # 호출할 Lambda 함수 이름
        payload={
            "key1": "value1",
            "key2": "value2",
        },  # Lambda 함수에 전달할 JSON payload
        log_type='Tail',  # 로그 출력 설정
        aws_conn_id='aws_default',  # Airflow에 설정된 AWS 연결 ID
        region_name='us-east-1',  # Lambda 함수가 있는 AWS 리전
    )

    invoke_lambda
