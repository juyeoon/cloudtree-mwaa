import boto3
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
from utils.slack_helpers import send_slack_message_fail, send_slack_message_success
from utils import config as cfg


def execute_athena_query(database_name, file_name):
    """
    S3에서 SQL 파일을 읽고 Athena로 실행
    """
    # S3
    query_key = f"{cfg.ATHENA_QUERY_PREFIX}/{file_name}.sql"
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=cfg.ATHENA_QUERY_BUCKET, Key=query_key)
    query = response["Body"].read().decode("utf-8")
    # Athena
    athena = boto3.client("athena")
    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database_name},
        ResultConfiguration={
            "OutputLocation": f"s3://{cfg.ATHENA_QUERY_OUTPUT_BUCKET}/{cfg.ATHENA_QUERY_OUTPUT_PREFIX}/"
        },
    )


def create_table(database_name, table_name, retry=3, retry_delay=10):
    """
    table 생성
    """
    return PythonOperator(
        task_id=f"create_table_{database_name}_{table_name}",
        python_callable=execute_athena_query,
        op_kwargs={
            "file_name": f'create_table_{table_name}',
            "database_name": database_name,
        },
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=send_slack_message_fail,
        on_failure_callback=send_slack_message_fail,
        on_success_callback=send_slack_message_success,
    )


def drop_table(database_name, table_name, retry=3, retry_delay=10):
    """
    table 삭제
    """
    return AthenaOperator(
        task_id=f"drop_table_{database_name}_{table_name}",
        query=f"DROP TABLE IF EXISTS {table_name}",
        database=database_name,
        output_location=f"s3://{cfg.ATHENA_QUERY_OUTPUT_BUCKET}/{cfg.ATHENA_QUERY_OUTPUT_PREFIX}/",
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=send_slack_message_fail,
        on_failure_callback=send_slack_message_fail,
        on_success_callback=send_slack_message_success,
    )


def msck_repair_table(database_name, table_name, retry=5, retry_delay=5):
    """
    table partition 갱신
    """
    return AthenaOperator(
        task_id=f"msck_repair_{database_name}_{table_name}",
        query=f"MSCK REPAIR TABLE {table_name}",
        database=database_name,
        output_location=f"s3://{cfg.ATHENA_QUERY_OUTPUT_BUCKET}/{cfg.ATHENA_QUERY_OUTPUT_PREFIX}/",
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=send_slack_message_fail,
        on_failure_callback=send_slack_message_fail,
        on_success_callback=send_slack_message_success,
    )
