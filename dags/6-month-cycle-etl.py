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
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

"""
6개월 주기 적재: 도서관 정보(lib), 버스정류장(bus), 지하철역(sta), 도시공원(park)
"""

# ==================================================================== slack alert ====================================================================


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


# Slack 메시지 생성 함수
def send_start_end_message(type, **context):
    """
    DAG 시작 시 Slack 메시지를 전송합니다.
    """
    logging.info(f"dag: {type}")
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']

    execution_date_kst = execution_date + timedelta(hours=9)
    execution_date_kst_str = execution_date_kst.strftime('%Y-%m-%d %H:%M:%S')  # KST 형식 문자열

    payload = {"text": f":rocket: DAG `{dag_id}` {type}!\n실행 시간: {execution_date_kst_str}"}
    send_slack_message(payload)


# ==================================================================== task template ====================================================================


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


def invoke_lambda(func_name, payload=None, retry=3, retry_delay=1):
    """
    AWS Lambda 호출
    """
    return LambdaInvokeFunctionOperator(
        task_id=f"invoke_lambda_{func_name}",
        function_name=func_name,
        payload=json.dumps(payload),
        retries=retry,
        retry_delay=timedelta(minutes=retry_delay),
        on_retry_callback=send_slack_message_fail,
        on_failure_callback=send_slack_message_fail,
        on_success_callback=send_slack_message_success,
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
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=send_slack_message_fail,
        on_failure_callback=send_slack_message_fail,
        on_success_callback=send_slack_message_success,
    )


def run_glue_job(job_name, retry=3, retry_delay=30):
    """
    glue job 실행
    """
    return GlueJobOperator(
        task_id=f"run_glue_job_{job_name}",
        job_name=job_name,
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=send_slack_message_fail,
        on_failure_callback=send_slack_message_fail,
        on_success_callback=send_slack_message_success,
    )


# ==================================================================== DAG ====================================================================

with DAG(
    dag_id="six-month-cycle-ETL-to-L1",
    start_date=days_ago(1),
    # schedule_interval='10 15 17 6,12 *', # 6월 18일, 12월 18일 오전 12시 10분
    schedule_interval=None,
    catchup=False,
) as dag:

    seoul_key = Variable.get("DATA_SEOUL_API_KEY")
    library_key = Variable.get("DATA4LIBRARY_API_KEY")
    districts_kor_eng = json.loads(Variable.get("DISTRICTS_KOR_ENG"))

    """
    lib: 도서관 정보
    bus: 버스정류장
    sta: 지하철역
    park: 도시공원
    """

    lib_dict = {
        'raw_db_nm': 'cloudtree_raw_db',
        'trans_db_nm': 'cloudtree_transformed_db',
        'raw_table_nm': 'library_data_raw',
        'trans_table_nm': 'library_data',
        'trans_job_nm': 'library_data_job',
        'api_lambda_nm': 'getLibDataAPI',
        'lambda_payload': {"districts": list(districts_kor_eng.keys()), "key": library_key},
    }

    bus_dict = {
        'raw_db_nm': 'cloudtree_raw_db',
        'trans_db_nm': 'cloudtree_transformed_db',
        'raw_table_nm': 'bus_stop_loc_raw',
        'trans_table_nm': 'bus_stop_loc',
        'trans_job_nm': 'bus_stop_loc_job',
        'api_lambda_nm': 'getBusStopLocAPI',
        'lambda_payload': {"key": seoul_key},
    }

    sta_dict = {
        'raw_db_nm': 'cloudtree_raw_db',
        'trans_db_nm': 'cloudtree_transformed_db',
        'raw_table_nm': 'subway_station_loc_raw',
        'trans_table_nm': 'subway_station_loc',
        'trans_job_nm': 'subway_station_loc_job',
        'api_lambda_nm': 'getSubwayStationLocAPI',
        'lambda_payload': {"key": seoul_key},
    }

    park_dict = {
        'raw_db_nm': 'cloudtree_raw_db',
        'trans_db_nm': 'cloudtree_transformed_db',
        'raw_table_nm': 'city_park_info_raw',
        'trans_table_nm': 'city_park_info',
        'trans_job_nm': 'city_park_info_job',
    }

    # ============================ start task ============================

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=send_start_end_message,
        provide_context=True,  # Context 전달
        op_kwargs={
            'type': 'start',
        },
    )

    # ============================ lib: create table ============================

    # with TaskGroup(group_id='create_lib_raw_table') as create_lib_raw_table:
    #     ct_sql_file_nm = f"create_table_{lib_dict['raw_table_nm']}"
    #     fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
    #     fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
    #     create_table_task = create_table(fetch_s3_task_id, lib_dict['raw_db_nm'])
    #     # TaskGroup 의존성 설정
    #     fetch_s3_task >> create_table_task

    # with TaskGroup(group_id='create_lib_trans_table') as create_lib_trans_table:
    #     ct_sql_file_nm = f"create_table_{lib_dict['trans_table_nm']}"
    #     fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
    #     fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
    #     create_table_task = create_table(fetch_s3_task_id, lib_dict['trans_db_nm'])
    #     # TaskGroup 의존성 설정
    #     fetch_s3_task >> create_table_task

    # ============================ lib: load to L1 ============================

    with TaskGroup(group_id='etl_lib') as etl_lib:
        invoke_api_lambda = invoke_lambda(lib_dict['api_lambda_nm'], lib_dict['lambda_payload'])
        msck_raw = update_data_catalog(lib_dict['raw_db_nm'], lib_dict['raw_table_nm'])
        run_trans_job = run_glue_job(lib_dict['trans_job_nm'])
        msck_trans = update_data_catalog(lib_dict['trans_db_nm'], lib_dict['trans_table_nm'])
        # TaskGroup 의존성 설정
        invoke_api_lambda >> msck_raw >> run_trans_job >> msck_trans

    # ============================ bus: create table ============================

    # with TaskGroup(group_id='create_bus_raw_table') as create_bus_raw_table:
    #     ct_sql_file_nm = f"create_table_{bus_dict['raw_table_nm']}"
    #     fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
    #     fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
    #     create_table_task = create_table(fetch_s3_task_id, bus_dict['raw_db_nm'])
    #     # TaskGroup 의존성 설정
    #     fetch_s3_task >> create_table_task

    # with TaskGroup(group_id='create_bus_trans_table') as create_bus_trans_table:
    #     ct_sql_file_nm = f"create_table_{bus_dict['trans_table_nm']}"
    #     fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
    #     fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
    #     create_table_task = create_table(fetch_s3_task_id, bus_dict['trans_db_nm'])
    #     # TaskGroup 의존성 설정
    #     fetch_s3_task >> create_table_task

    # ============================ bus: load to L1 ============================

    with TaskGroup(group_id='etl_bus') as etl_bus:
        invoke_api_lambda = invoke_lambda(bus_dict['api_lambda_nm'], bus_dict['lambda_payload'])
        msck_raw = update_data_catalog(bus_dict['raw_db_nm'], bus_dict['raw_table_nm'])
        run_trans_job = run_glue_job(bus_dict['trans_job_nm'])
        msck_trans = update_data_catalog(bus_dict['trans_db_nm'], bus_dict['trans_table_nm'])
        # TaskGroup 의존성 설정
        invoke_api_lambda >> msck_raw >> run_trans_job >> msck_trans

    # ============================ sta: create table ============================

    # with TaskGroup(group_id='create_sta_raw_table') as create_sta_raw_table:
    #     ct_sql_file_nm = f"create_table_{sta_dict['raw_table_nm']}"
    #     fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
    #     fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
    #     create_table_task = create_table(fetch_s3_task_id, sta_dict['raw_db_nm'])
    #     # TaskGroup 의존성 설정
    #     fetch_s3_task >> create_table_task

    # with TaskGroup(group_id='create_sta_trans_table') as create_sta_trans_table:
    #     ct_sql_file_nm = f"create_table_{sta_dict['trans_table_nm']}"
    #     fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
    #     fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
    #     create_table_task = create_table(fetch_s3_task_id, sta_dict['trans_db_nm'])
    #     # TaskGroup 의존성 설정
    #     fetch_s3_task >> create_table_task

    # ============================ sta: load to L1 ============================

    with TaskGroup(group_id='etl_sta') as etl_sta:
        invoke_api_lambda = invoke_lambda(sta_dict['api_lambda_nm'], sta_dict['lambda_payload'])
        msck_raw = update_data_catalog(sta_dict['raw_db_nm'], sta_dict['raw_table_nm'])
        run_trans_job = run_glue_job(sta_dict['trans_job_nm'])
        msck_trans = update_data_catalog(sta_dict['trans_db_nm'], sta_dict['trans_table_nm'])
        # TaskGroup 의존성 설정
        invoke_api_lambda >> msck_raw >> run_trans_job >> msck_trans

    # ============================ park: create table ============================

    # with TaskGroup(group_id='create_park_raw_table') as create_park_raw_table:
    #     ct_sql_file_nm = f"create_table_{park_dict['raw_table_nm']}"
    #     fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
    #     fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
    #     create_table_task = create_table(fetch_s3_task_id, park_dict['raw_db_nm'])
    #     # TaskGroup 의존성 설정
    #     fetch_s3_task >> create_table_task

    # with TaskGroup(group_id='create_park_trans_table') as create_park_trans_table:
    #     ct_sql_file_nm = f"create_table_{park_dict['trans_table_nm']}"
    #     fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
    #     fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
    #     create_table_task = create_table(fetch_s3_task_id, park_dict['trans_db_nm'])
    #     # TaskGroup 의존성 설정
    #     fetch_s3_task >> create_table_task

    # ============================ park: load to L1 ============================

    with TaskGroup(group_id='etl_park') as etl_park:
        msck_raw = update_data_catalog(park_dict['raw_db_nm'], park_dict['raw_table_nm'])
        run_trans_job = run_glue_job(park_dict['trans_job_nm'])
        msck_trans = update_data_catalog(park_dict['trans_db_nm'], park_dict['trans_table_nm'])
        # TaskGroup 의존성 설정
        msck_raw >> run_trans_job >> msck_trans

    # ============================ end task ============================

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=send_start_end_message,
        provide_context=True,  # Context 전달
        op_kwargs={
            'type': 'end',
        },
    )

    # task group 의존성 설정
    # start_task >> (create_lib_raw_table, create_lib_trans_table) >> etl_lib
    # start_task >> (create_bus_raw_table, create_bus_trans_table) >> etl_bus
    # start_task >> (create_sta_raw_table, create_sta_trans_table) >> etl_sta
    # start_task >> (create_park_raw_table, create_park_trans_table) >> etl_park
    start_task >> [etl_lib, etl_bus, etl_sta, etl_park] >> end_task
