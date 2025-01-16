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
1개월 주기 적재: 인기대출도서 목록(bll), 서울시 문화행사 목록(cul)
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


def invoke_lambda(func_name, payload=None, retry=5, retry_delay=1):
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
    dag_id="a-one-month-cycle-ETL-to-L2",
    start_date=days_ago(1),
    # schedule_interval='10 15 1 * *', # 매달 1일 오전 12시 10분
    schedule_interval=None,
    catchup=False,
) as dag:

    seoul_key = Variable.get("DATA_SEOUL_API_KEY")
    library_key = Variable.get("DATA4LIBRARY_API_KEY")
    districts_kor_eng = json.loads(Variable.get("DISTRICTS_KOR_ENG"))

    last_month_date = datetime.now().date() - relativedelta(months=1)
    last_month = (datetime.now().date() - relativedelta(months=1)).strftime("%Y%m")

    """
    bll: 인기대출도서
    cul: 서울문화행사
    """

    bll_dict = {
        'raw_db_nm': 'cloudtree_raw_db',
        'trans_db_nm': 'cloudtree_transformed_db',
        'raw_table_nm': 'best_loan_list_raw',
        'trans_table_nm': 'best_loan_list',
        'trans_job_nm': 'best_loan_list_job',
        'api_lambda_nm': 'getBestLoanAPI',
        'lambda_payload': {
            "districts": list(districts_kor_eng.keys()),
            "key": library_key,
            "periodStart": last_month,
            "periodEnd": last_month,
        },
    }

    cul_dict = {
        'raw_db_nm': 'cloudtree_raw_db',
        'trans_db_nm': 'cloudtree_transformed_db',
        'raw_parse_table_nm': 'cultural_event_parse_raw',
        'raw_info_table_nm': 'cultural_event_info_raw',
        'trans_table_nm': 'cultural_event_info',
        'keyword_job_nm': 'cultural_event_keyword_job',
        'trans_job_nm': 'best_loan_list_job',
        'api_lambda_nm': 'getCulEventAPI',
        'lambda_payload': {"districts": list(districts_kor_eng.keys()), "key": seoul_key},
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

    # ============================ bll: create table ============================

    with TaskGroup(group_id='create_bll_raw_table') as create_bll_raw_table:
        ct_sql_file_nm = f"create_table_{bll_dict['raw_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = create_table(fetch_s3_task_id, bll_dict['raw_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    with TaskGroup(group_id='create_bll_trans_table') as create_bll_trans_table:
        ct_sql_file_nm = f"create_table_{bll_dict['trans_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = create_table(fetch_s3_task_id, bll_dict['trans_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    # ============================ bll: load to L2 ============================

    with TaskGroup(group_id='etl_bbl') as etl_bbl:
        invoke_api_lambda = invoke_lambda(bll_dict['api_lambda_nm'], bll_dict['lambda_payload'])
        msck_raw = update_data_catalog(bll_dict['raw_db_nm'], bll_dict['raw_table_nm'])
        run_trans_job = run_glue_job(bll_dict['trans_job_nm'])
        msck_trans = update_data_catalog(bll_dict['trans_db_nm'], bll_dict['trans_table_nm'])
        # TaskGroup 의존성 설정
        invoke_api_lambda >> msck_raw >> run_trans_job >> msck_trans

    # ============================ cul: create table ============================

    with TaskGroup(group_id='create_cul_raw_parse_table') as create_cul_raw_parse_table:
        ct_sql_file_nm = f"create_table_{cul_dict['raw_parse_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = create_table(fetch_s3_task_id, cul_dict['raw_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    with TaskGroup(group_id='create_cul_raw_info_table') as create_cul_raw_info_table:
        ct_sql_file_nm = f"create_table_{cul_dict['raw_info_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = create_table(fetch_s3_task_id, cul_dict['raw_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    with TaskGroup(group_id='create_cul_trans_table') as create_cul_trans_table:
        ct_sql_file_nm = f"create_table_{cul_dict['trans_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = fetch_query_s3(fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = create_table(fetch_s3_task_id, cul_dict['trans_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    # ============================ cul: load to L2 ============================

    with TaskGroup(group_id='etl_cul') as etl_cul:
        invoke_api_lambda = invoke_lambda(cul_dict['api_lambda_nm'], cul_dict['lambda_payload'])
        msck_parse_raw = update_data_catalog(cul_dict['raw_db_nm'], cul_dict['raw_parse_table_nm'])
        run_keyword_job = run_glue_job(cul_dict['keyword_job_nm'])
        msck_info_raw = update_data_catalog(cul_dict['raw_db_nm'], cul_dict['raw_info_table_nm'])
        run_trans_job = run_glue_job(cul_dict['trans_job_nm'])
        msck_trans = update_data_catalog(cul_dict['trans_db_nm'], cul_dict['trans_table_nm'])
        # TaskGroup 의존성 설정
        invoke_api_lambda >> msck_parse_raw >> run_keyword_job >> msck_info_raw >> run_trans_job >> msck_trans

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
    start_task >> (create_bll_raw_table, create_bll_trans_table) >> etl_bbl
    start_task >> (create_cul_raw_parse_table, create_cul_raw_info_table, create_cul_trans_table) >> etl_cul
    [etl_bbl, etl_cul] >> end_task
