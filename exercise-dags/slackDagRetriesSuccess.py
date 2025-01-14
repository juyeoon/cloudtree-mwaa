from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import requests
from datetime import datetime, timedelta

"""
# task 실패, retry 실패, 성공하면 slack으로 메시지 보내는 DAG
"""

# Slack Webhook URL 설정
SLACK_WEBHOOK_URL = "YOUR_SLACK_API"


# Slack 메시지 전송 함수
def send_slack_message_retry(context):
    """
    Slack으로 메시지를 전송하는 함수. 실행 중인 DAG ID와 Task ID를 컨텍스트에서 가져옵니다.
    """
    # Airflow 컨텍스트에서 DAG ID와 Task ID 가져오기
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
    try_number = context['task_instance'].try_number  # 현재 재시도 번호
    max_tries = context['task'].retries + 1  # 최대 재시도 횟수 (기본 시도 + 재시도)

    # Slack 메시지 구성
    message = {
        "text": f"""
        :x: *Airflow DAG 실패 알림*
        - DAG: `{dag_id}`
        - Task: `{task_id}`
        - 실행 시간: `{execution_date}`
        - 실행 시도: `{try_number}/{max_tries}`
        - <{log_url}|Task Log>
        """
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=message)

    # 응답 상태 확인
    if response.status_code != 200:
        raise ValueError(f"Slack 메시지 전송 실패: {response.status_code}, {response.text}")


# Slack 메시지 전송 함수
def send_slack_message_success(context):
    """
    Slack으로 성공 메시지를 전송하는 함수. 실행 중인 DAG ID와 Task ID를 컨텍스트에서 가져옵니다.
    """
    # Airflow 컨텍스트에서 DAG ID와 Task ID 가져오기
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url

    # Slack 메시지 구성
    message = {
        "text": f"""
        :white_check_mark: *Airflow DAG 성공 알림*
        - DAG: `{dag_id}`
        - Task: `{task_id}`
        - 실행 시간: `{execution_date}`
        - <{log_url}|Task Log>
        """
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=message)

    # 응답 상태 확인
    if response.status_code != 200:
        raise ValueError(f"Slack 메시지 전송 실패: {response.status_code}, {response.text}")


def random_success_or_failure():
    """
    랜덤으로 성공하거나 실패하는 함수.
    """
    if random.choice([True, False]):
        print("Task succeeded!")
    else:
        raise Exception("Task failed!")


# 시작, 종료 로그
def log_state(state):
    if state == 'start':
        print("DAG Execution Started: Tasks are about to run!")
    elif state == 'end':
        print("DAG Execution Finished: All tasks are done!")


# DAG 정의
with DAG(
    dag_id="slack_task_success_example",
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    schedule_interval=None,  # 실행 주기: None으로 설정하여 수동 실행
    catchup=False,
) as dag:

    # 작업 시작
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: log_state('start'),
    )

    failure_task = PythonOperator(
        task_id="task_3",
        python_callable=random_success_or_failure,  # 실패를 유도하는 작업
        retries=5,  # 태스크별 재시도 횟수 설정
        retry_delay=timedelta(seconds=30),  # 태스크별 재시도 대기 시간 설정
        on_retry_callback=send_slack_message_retry,  # 재시도 때마다 실행
        on_failure_callback=send_slack_message_retry,  # 최종 실패 시 호출
        on_success_callback=send_slack_message_success,  # 성공 시 호출
    )

    # 성공/실패와 관계없이 실행될 작업
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: log_state('end'),
        trigger_rule="all_done",  # 모든 작업 완료 후 실행
    )

    # 작업 간 순서 정의
    start >> failure_task >> end
