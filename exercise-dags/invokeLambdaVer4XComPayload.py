import json
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.operators.python import PythonOperator
from datetime import datetime

"""
# AWS Lambda를 호출하는 DAG. payload를 lambda로 보내고 XCOM에 저장된 lambda의 return 값을 출력
"""

# DAG 정의
with DAG(
    dag_id="use_lambda_ret_value",
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    schedule_interval=None,  # 실행 주기: None으로 설정하여 수동 실행
    catchup=False,
) as dag:
    # Lambda 함수 호출 작업 정의
    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda",
        function_name="mwaaTestFunc",  # 호출할 Lambda 함수 이름
        payload=json.dumps({"key1": "val1", "key2": "val2", "key3": "val3"}),  # Lambda로 보낼 페이로드
    )

    # Lambda 반환값 처리
    def process_lambda_result(**kwargs):
        ti = kwargs['ti']  # task instance
        # XCom에서 Lambda 반환값 가져오기
        lambda_result = ti.xcom_pull(task_ids='invoke_lambda')
        print(f"Lambda Result raw: {lambda_result}")
        lambda_result_dict = json.loads(lambda_result)
        decoded_body = json.loads(lambda_result_dict['body'])
        print(f"Lambda Result: {decoded_body}")

    # PythonOperator를 사용해 Lambda 결과를 처리하는 태스크 정의
    process_result_task = PythonOperator(
        task_id="process_lambda_result",
        python_callable=process_lambda_result,
        provide_context=True,  # Task Instance 정보 전달
    )

    # 태스크 의존성 설정
    invoke_lambda >> process_result_task
