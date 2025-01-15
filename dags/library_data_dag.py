import json
import boto3
import requests
import logging
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

# =========== data 3 : 도서관 ===========


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


def read_sql_from_s3(bucket_name, key):
    """
    S3에서 파일 읽기
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read().decode("utf-8")


def fetch_query_s3(fetch_s3_task_id, file_name):
    """
    S3에서 create table SQL 파일 읽기
    """
    return PythonOperator(
        task_id=fetch_s3_task_id,
        python_callable=read_sql_from_s3,
        op_kwargs={
            "bucket_name": "cloudtree-mwaa-query",
            "key": f"athena-query/{file_name}.sql",
        },
    )


def create_table(fetch_s3_task_id, database_name):
    """
    XCom에 저장된 create table문을 Athena로 실행
    """
    return AthenaOperator(
        task_id=f"create_table_by_{fetch_s3_task_id}",
        query=f"{{{{ ti.xcom_pull(task_ids='{fetch_s3_task_id}') }}}}",  # XCom에서 쿼리 내용 가져오기
        database=database_name,
        output_location="s3://cloudtree-athena-query-result/mwaa-dag-query-results/",
    )


def invoke_lambda(func_name, payload=None, retry=5, retry_delay=1):
    """
    AWS Lambda 호출
    """
    return LambdaInvokeFunctionOperator(
        task_id=f"invoke_lambda_{func_name}",
        function_name=func_name,
        payload=json.dumps(payload),
        # retries=retry,
        # retry_delay=timedelta(minutes=retry_delay),
        # on_retry_callback=send_slack_message_fail,
        # on_failure_callback=send_slack_message_fail,
        # on_success_callback=send_slack_message_success,
    )


def update_data_catalog(database_name, table_name, retry=5, retry_delay=5):
    """
    data catalog 갱신
    """
    return AthenaOperator(
        task_id=f"msck_repair_{table_name}",
        query=f"MSCK REPAIR TABLE {table_name}",
        database=database_name,
        output_location="s3://cloudtree-athena-query-result/mwaa-dag-query-results/",
        # retries=retry,
        # retry_delay=timedelta(seconds=retry_delay),
        # on_retry_callback=send_slack_message_fail,
        # on_failure_callback=send_slack_message_fail,
        # on_success_callback=send_slack_message_success,
    )


def run_glue_job(job_name, retry=3, retry_delay=30):
    """
    glue job 실행
    """
    return GlueJobOperator(
        task_id=f"run_glue_job_{job_name}",
        job_name=job_name,
        # retries=retry,
        # retry_delay=timedelta(seconds=retry_delay),
        # on_retry_callback=send_slack_message_fail,
        # on_failure_callback=send_slack_message_fail,
        # on_success_callback=send_slack_message_success,
    )


# DAG 정의
with DAG(
    dag_id="library-data-dag",
    start_date=days_ago(1),
    # schedule_interval='10 0 1 * *',
    schedule_interval=None,
    catchup=False,
) as dag:

    districts = json.loads(Variable.get("DISTRICTS"))
    seoul_key = Variable.get("DATA_SEOUL_API_KEY")
    library_key = Variable.get("DATA4LIBRARY_API_KEY")

    # =========== data 3 : 도서관 ===========

    raw_library_data_table = "library_data_raw"
    raw_library_data_database_name = "cloudtree_raw_db"
    raw_library_data_sql_file_name = f"create_table_{raw_library_data_table}"
    raw_library_data_fetch_s3_task_id = f"fetch_sql_{raw_library_data_sql_file_name}"
    raw_library_data_table_ddl = fetch_query_s3(
        raw_library_data_fetch_s3_task_id, f"create_table_{raw_library_data_table}"
    )
    raw_library_data_create_table_by_ddl = create_table(
        raw_library_data_fetch_s3_task_id, raw_library_data_database_name
    )

    trans_library_data_table = "library_data"
    trans_library_data_database_name = "cloudtree_transformed_db"
    trans_library_data_sql_file_name = f"create_table_{trans_library_data_table}"
    trans_library_data_fetch_s3_task_id = f"fetch_sql_{trans_library_data_sql_file_name}"
    trans_library_data_table_ddl = fetch_query_s3(
        trans_library_data_fetch_s3_task_id, f"create_table_{trans_library_data_table}"
    )
    trans_library_data_create_table_by_ddl = create_table(
        trans_library_data_fetch_s3_task_id, trans_library_data_database_name
    )

    invoke_library_data_API = invoke_lambda("getLibDataAPI", {"districts": districts, "key": library_key})
    msck_library_data_raw = update_data_catalog("cloudtree_raw_db", "library_data_raw")
    run_glue_library_data = run_glue_job("library_data_job")
    msck_library_data_trans = update_data_catalog("cloudtree_transformed_db", "library_data")

    # task 의존성 설정

    (
        raw_library_data_table_ddl
        >> raw_library_data_create_table_by_ddl
        >> trans_library_data_table_ddl
        >> trans_library_data_create_table_by_ddl
        >> invoke_library_data_API
        >> msck_library_data_raw
        >> run_glue_library_data
        >> msck_library_data_trans
    )
