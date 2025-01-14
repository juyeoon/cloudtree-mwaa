from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

"""
# task가 실패하면 slack으로 메시지 보내는 DAG
"""


# Slack Webhook URL 설정
SLACK_WEBHOOK_URL = "YOUR_SLACK_API"


# Slack 메시지 전송 함수
def send_slack_message(**context):
    """
    Slack으로 메시지를 전송하는 함수. 실행 중인 DAG ID와 Task ID를 컨텍스트에서 가져옵니다.
    """
    # Airflow 컨텍스트에서 DAG ID와 Task ID 가져오기
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url

    # Slack 메시지 구성
    message = {
        "text": f"""
        :x: *Airflow DAG 실패 알림*
        - DAG: `{dag_id}`
        - Task: `{task_id}`
        - 실행 시간: `{execution_date}`
        - [로그 보기]({log_url})
        """
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=message)

    # 응답 상태 확인
    if response.status_code != 200:
        raise ValueError(f"Slack 메시지 전송 실패: {response.status_code}, {response.text}")


# 시작, 종료 로그
def log_state(state):
    if state == 'start':
        print("DAG Execution Started: Tasks are about to run!")
    elif state == 'end':
        print("DAG Execution Finished: All tasks are done!")


# DAG 정의
with DAG(
    dag_id="slack_error_notification_example",
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

    # 실패 시 slack 알림 작업
    send_slack_alert = PythonOperator(
        task_id="send_slack_alert",
        python_callable=send_slack_message,
        trigger_rule="one_failed",  # 선행 작업 중 하나라도 실패하면 실행
        provide_context=True,  # 컨텍스트를 함수에 전달
    )

    # 성공/실패와 관계없이 실행될 작업
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: log_state('end'),
        trigger_rule="all_done",  # 모든 작업 완료 후 실행
    )

    # 작업 간 순서 정의
    start >> invoke_lambda >> send_slack_alert >> end
