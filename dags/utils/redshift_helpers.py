import boto3
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
from utils import config as cfg


def execute_redshift_query(file_name):
    """
    S3에서 SQL 파일을 읽고 redshift로 실행
    """
    # S3
    query_key = f"{cfg.REDSHIFT_QUERY_PREFIX}/{file_name}.sql"
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=cfg.REDSHIFT_QUERY_BUCKET, Key=query_key)
    query = response["Body"].read().decode("utf-8")
    # redshift
    redshift_hook = RedshiftSQLHook(redshift_conn_id=cfg.REDSHIFT_CONN_ID)
    results = redshift_hook.get_records(query)
    for row in results:
        print(row)


def select_table():
    return PythonOperator(
        task_id='execute_redshift_query',
        python_callable=execute_redshift_query,
        op_kwargs={"file_name": f'create_table_{table_name}'},
    )
