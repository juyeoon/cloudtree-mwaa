import boto3
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.operators.python import PythonOperator
from datetime import datetime

"""
# AWS SNS에 등록된 TOPIC을 사용하여 Task가 실패했을 때 이메일을 보내는 DAG
"""


# 시작, 종료 로그
def log_state(state):
    if state == 'start':
        print("DAG Execution Started: Tasks are about to run!")
    elif state == 'end':
        print("DAG Execution Finished: All tasks are done!")


# SNS 알림 함수 정의
def send_sns_notification(**kwargs):
    sns_client = boto3.client("sns", region_name="ap-northeast-2")  # AWS 리전 설정
    topic_arn = "arn:aws:sns:ap-northeast-2:YOUR_AWS_ACCOUNT:SNS_TOPIC_NAME"  # SNS 주제 ARN: 작성 필요

    # 실패한 Task와 DAG 정보 포함
    message = f"""
    DAG: {kwargs['dag'].dag_id}
    Task: {kwargs['task_instance'].task_id}
    Execution Time: {kwargs['execution_date']}
    Log URL: {kwargs['task_instance'].log_url}
    """

    # SNS 메시지 전송
    sns_client.publish(TopicArn=topic_arn, Message=message, Subject="DAG Failure Notification")


# DAG 정의
with DAG(
    dag_id="sns_error_notification_example",
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    schedule_interval=None,  # 실행 주기: None으로 설정하여 수동 실행
    catchup=False,
) as dag:

    # 작업 시작
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: log_state('start'),
    )

    # Lambda 함수 호출 작업 정의
    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda",
        function_name="mwaaTestFunc",  # 호출할 Lambda 함수 이름
    )

    # 실패 시 SNS 알림 작업
    error_notification = PythonOperator(
        task_id="error_notification",
        python_callable=send_sns_notification,
        trigger_rule="one_failed",  # 선행 작업 중 하나라도 실패하면 실행
    )

    # 성공/실패와 관계없이 실행될 작업
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: log_state('end'),
        trigger_rule="all_done",  # 모든 작업 완료 후 실행
    )

    # 작업 간 순서 정의
    start >> invoke_lambda >> error_notification >> end
