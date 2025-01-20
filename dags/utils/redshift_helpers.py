import boto3
import logging
from datetime import timedelta
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
from utils import config as cfg
from utils import slack_helpers as sh


def update_advanced_analysis_table(red_tables):
    """
    심화 분석 테이블 갱신. 심화 분석 테이블의 데이터를 갱신하는 함수 (create + insert)
    """
    redshift_hook = RedshiftSQLHook(redshift_conn_id=cfg.REDSHIFT_CONN_ID)
    for table in red_tables:
        # drop
        drop_table_query = f"DROP TABLE IF EXISTS {table};"
        logging.info(drop_table_query)
        redshift_hook.run(drop_table_query)
        # create
        # S3에서 SQL 파일 읽기
        query_key = f"{cfg.REDSHIFT_QUERY_PREFIX}/create_insert_table_{table}.sql"
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=cfg.REDSHIFT_QUERY_BUCKET, Key=query_key)
        create_insert_query = response["Body"].read().decode("utf-8")
        logging.info(create_insert_query)
        # SQL 명령문을 ";" 기준으로 분리
        queries = [query.strip() for query in create_insert_query.split(";") if query.strip()]
        logging.info(f"Found {len(queries)} SQL statements.")
        # SQL 명령문 순차 실행
        for i, query in enumerate(queries, 1):
            logging.info(f"Executing query {i}/{len(queries)}: {query}")
            try:
                redshift_hook.run(query)
                logging.info(f"Query {i} executed successfully.")
            except Exception as e:
                logging.error(f"Error executing query {i}: {e}")
                raise


def update_cloudtree_agg_table(agg_red_tables):
    """
    기본 분석 테이블 갱신. aggreated 버킷에 있는 데이터에 대해서 redshift 테이블 데이터를 갱신하는 함수 (create + copy)
    """
    redshift_hook = RedshiftSQLHook(redshift_conn_id=cfg.REDSHIFT_CONN_ID)
    for agg_table_name, red_table_name in agg_red_tables.items():
        # drop
        drop_table_query = f"DROP TABLE IF EXISTS {red_table_name};"
        logging.info(drop_table_query)
        redshift_hook.run(drop_table_query)
        # create
        # S3에서 SQL 파일 읽기
        query_key = f"{cfg.REDSHIFT_QUERY_PREFIX}/create_table_{red_table_name}.sql"
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=cfg.REDSHIFT_QUERY_BUCKET, Key=query_key)
        create_query = response["Body"].read().decode("utf-8")
        logging.info(create_query)
        redshift_hook.run(create_query)
        # copy
        copy_query = f"""
        COPY {red_table_name}
        FROM 's3://{cfg.AGG_BUCKET}/{agg_table_name}/'
        IAM_ROLE 'arn:aws:iam::{cfg.ACCOUNT_ID}:role/{cfg.REDSHIFT_COPY_ROLE_NAME}'
        FORMAT AS PARQUET;
        """
        logging.info(copy_query)
        redshift_hook.run(copy_query)


def task_load_agg_data_to_red(agg_red_tables, retry=5, retry_delay=10):
    """
    task: 기본 분석 테이블 데이터 갱신
    """
    return PythonOperator(
        task_id=f"load_agg_data_to_red",
        python_callable=update_cloudtree_agg_table,
        op_kwargs={"agg_red_tables": agg_red_tables},
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=sh.send_slack_message_fail,
        on_failure_callback=sh.send_slack_message_fail,
        on_success_callback=sh.send_slack_message_success,
    )


def task_update_analysis_table(red_tables, retry=5, retry_delay=10):
    """
    task: 심화 분석 테이블 데이터 갱신
    """
    return PythonOperator(
        task_id=f"update_analysis_table",
        python_callable=update_advanced_analysis_table,
        op_kwargs={"red_tables": red_tables},
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=sh.send_slack_message_fail,
        on_failure_callback=sh.send_slack_message_fail,
        on_success_callback=sh.send_slack_message_success,
    )
