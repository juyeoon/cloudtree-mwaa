import requests
import logging
from datetime import timedelta
from airflow.models import Variable


def send_slack_message(payload):
    """
    Slack 메시지를 전송하는 공통 함수 (mwaa)
    payload: Slack 메시지의 JSON 데이터
    """
    try:
        # MWAA에서 Airflow Variable로 Webhook URL 가져오기
        SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
        if not SLACK_WEBHOOK_URL:
            raise ValueError(
                "Slack Webhook URL이 설정되지 않았습니다. Airflow Variables를 확인하세요."
            )

        # Slack 메시지 전송
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            logging.error(
                f"Slack 메시지 전송 실패: {response.status_code}, {response.text}"
            )
            raise ValueError(f"Slack 메시지 전송 실패: {response.status_code}")

    except Exception as e:
        logging.error(f"Slack 메시지 전송 중 예외 발생: {e}")


def send_slack_message_success(context):
    """
    Slack으로 성공 메시지를 전송하는 함수
    """
    # task 정보
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    log_url = context["task_instance"].log_url

    # KST 변환: UTC + 9시간
    execution_date_kst = execution_date + timedelta(hours=9)
    execution_date_kst_str = execution_date_kst.strftime("%Y-%m-%d %H:%M:%S")

    # Slack 메시지 구성
    message = {
        "text": f"""
        :white_check_mark: *Airflow DAG 성공 알림*
        - DAG: `{dag_id}`
        - Task: `{task_id}`
        - 실행 시간 (KST): `{execution_date_kst_str}`
        - <{log_url}|Task Log>
        """
    }

    send_slack_message(message)


def send_slack_message_fail(context):
    """
    Slack으로 실패 메시지를 전송하는 함수
    """
    # task 정보
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    log_url = context["task_instance"].log_url
    try_number = context["task_instance"].try_number
    max_tries = context["task"].retries + 1

    # KST 변환: UTC + 9시간
    execution_date_kst = execution_date + timedelta(hours=9)
    execution_date_kst_str = execution_date_kst.strftime("%Y-%m-%d %H:%M:%S")

    # Slack 메시지 구성
    message = {
        "text": f"""
        :x: *Airflow DAG 실패 알림*
        - DAG: `{dag_id}`
        - Task: `{task_id}`
        - 실행 시간 (KST): `{execution_date_kst_str}`
        - 실행 시도: `{try_number}/{max_tries}`
        - <{log_url}|Task Log>
        """
    }
    send_slack_message(message)


# Slack 메시지 생성 함수
def send_dag_state_message(type, **context):
    """
    Slack으로 DAG 시작 및 종료 메시지를 전송하는 함수
    """
    # dag 정보
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]

    # KST 변환: UTC + 9시간
    execution_date_kst = execution_date + timedelta(hours=9)
    execution_date_kst_str = execution_date_kst.strftime("%Y-%m-%d %H:%M:%S")

    message = {
        "text": f":rocket: DAG `{dag_id}` {type}!\n실행 시간: {execution_date_kst_str}"
    }
    send_slack_message(message)
