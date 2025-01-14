import json
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import logging


def send_slack_message(payload):
    """
    Slack 메시지를 전송하는 공통 함수 (MWAA 버전).
    payload: Slack 메시지의 JSON 데이터
    """
    try:
        # MWAA에서 Airflow Variable로 Webhook URL 가져오기
        SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
        if not SLACK_WEBHOOK_URL:
            raise ValueError("Slack Webhook URL이 설정되지 않았습니다. Airflow Variables를 확인하세요.")

        # Slack 메시지 전송
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            logging.error(f"Slack 메시지 전송 실패: {response.status_code}, {response.text}")
            raise ValueError(f"Slack 메시지 전송 실패: {response.status_code}")

    except Exception as e:
        logging.error(f"Slack 메시지 전송 중 예외 발생: {e}")


def send_slack_message_success(context):
    """
    Slack으로 성공 메시지를 전송하는 함수
    """
    # Airflow 컨텍스트에서 DAG ID와 Task ID 가져오기
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url

    # KST 변환: UTC + 9시간
    execution_date_kst = execution_date + timedelta(hours=9)
    execution_date_kst_str = execution_date_kst.strftime('%Y-%m-%d %H:%M:%S')  # KST 형식 문자열

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
    # Airflow 컨텍스트에서 DAG ID와 Task ID 가져오기
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
    try_number = context['task_instance'].try_number
    max_tries = context['task'].retries + 1

    # KST 변환: UTC + 9시간
    execution_date_kst = execution_date + timedelta(hours=9)
    execution_date_kst_str = execution_date_kst.strftime('%Y-%m-%d %H:%M:%S')  # KST 형식 문자열

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


def invoke_lambda(func_name, payload=None, retry=5, retry_delay=1):
    return LambdaInvokeFunctionOperator(
        task_id=f"invoke_lambda_{func_name}",
        function_name=f"{func_name}",
        retries=retry,
        retry_delay=timedelta(minutes=retry_delay),
        payload=json.dumps(payload),  # Lambda로 보낼 페이로드
        on_retry_callback=send_slack_message_fail,  # 재시도 때마다 실행
        on_failure_callback=send_slack_message_fail,  # 최종 실패 시 호출
        on_success_callback=send_slack_message_success,  # 성공 시 호출
    )


# DAG 정의
with DAG(
    dag_id="step_1_invoke_get_api_lambda",
    start_date=days_ago(1),
    # schedule_interval='10 0 1 * *',
    schedule_interval=None,
    catchup=False,
) as dag:

    districts = json.loads(Variable.get("DISTRICTS"))
    seoul_key = Variable.get("DATA_SEOUL_API_KEY")
    library_key = Variable.get("DATA4LIBRARY_API_KEY")

    invoke_bus_stop = invoke_lambda("getBusStopLocAPI", {"key": seoul_key})
    invoke_subway_station = invoke_lambda("getSubwayStationLocAPI", {"key": seoul_key})

    invoke_cultural_event = invoke_lambda("getCulEventAPI", {"districts": districts, "key": seoul_key})
    invoke_library_data = invoke_lambda("getLibDataAPI", {"districts": districts, "key": library_key})

    # 인기 대출 데이터 필요

    # 태스크 의존성 설정
