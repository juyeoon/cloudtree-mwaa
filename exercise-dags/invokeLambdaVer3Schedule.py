from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from datetime import datetime

"""
# AWS Lambda를 호출하는 DAG. Cron으로 실행 주기 설정
"""

# DAG 정의
with DAG(
    dag_id="simple_lambda_invoke_cron",
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    schedule_interval='10 5 * * *',  # 실행 주기 (UTC)
    catchup=False,
) as dag:
    # Lambda 함수 호출 작업 정의
    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda",
        function_name="mwaaTestFunc",  # 호출할 Lambda 함수 이름
    )
