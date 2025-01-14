from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from datetime import datetime

"""
# AWS Lambda를 호출하는 DAG. 필수적인 요소만 사용
"""

# DAG 정의
with DAG(
    dag_id="simple_lambda_invoke",
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    schedule_interval=None,  # 실행 주기: None으로 설정하여 수동 실행
    catchup=False,
) as dag:
    # Lambda 함수 호출 작업 정의
    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda",
        function_name="mwaaTestFunc",  # 호출할 Lambda 함수 이름
    )
