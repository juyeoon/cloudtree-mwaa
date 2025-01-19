import boto3
import logging
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
from utils import config as cfg


def red_helper_test():
    return PythonOperator(
        task_id=f"red_helper_test",
        python_callable=update_advanced_analysis_table,
        op_kwargs={
            # "red_table_name": red_table_name,
            # "agg_table_name": agg_table_name,
        },
    )


def update_advanced_analysis_table(table_name="test_analysis_results"):
    """ """
    redshift_hook = RedshiftSQLHook(redshift_conn_id=cfg.REDSHIFT_CONN_ID)
    # 테이블 존재 여부 확인
    check_table_sql = f"""
    SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}';
    """
    check_result = redshift_hook.get_records(check_table_sql)
    logging.info(check_result)
    if check_result:
        # drop
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        logging.info(drop_table_query)
        redshift_hook.run(drop_table_query)
    # create
    # S3에서 SQL 파일 읽기
    query_key = f"{cfg.REDSHIFT_QUERY_PREFIX}/create_insert_table_{table_name}.sql"
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=cfg.REDSHIFT_QUERY_BUCKET, Key=query_key)
    create_insert_query = response["Body"].read().decode("utf-8")
    logging.info(create_insert_query)
    # SQL 명령문을 ";" 기준으로 분리
    queries = [
        query.strip() for query in create_insert_query.split(";") if query.strip()
    ]
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


# def update_spectrum_schema(schema_name="spectrum_schema"):
#     """ """
#     redshift_hook = RedshiftSQLHook(redshift_conn_id=cfg.REDSHIFT_CONN_ID)
#     # 스키마 존재 여부 확인 쿼리
#     check_schema_sql = f"""
#     SELECT schema_name
#     FROM information_schema.schemata
#     WHERE schema_name = '{schema_name}';
#     """
#     check_result = redshift_hook.get_records(check_schema_sql)
#     if check_result:
#         # drop
#         drop_schema_query = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;"
#         redshift_hook.run(drop_schema_query)
#     # create
#     create_schema_query = f"""
#     CREATE EXTERNAL SCHEMA {schema_name}
#     FROM DATA CATALOG
#     DATABASE '{cfg.TRANS_DB}'
#     IAM_ROLE 'arn:aws:iam::{cfg.ACCOUNT_ID}:role/{cfg.REDSHIFT_COPY_ROLE_NAME}'
#     REGION 'ap-northeast-2';
#     """
#     redshift_hook.run(create_schema_query)


def update_cloudtree_agg_table(red_table_name, agg_table_name):
    """
    aggreated 버킷에 있는 데이터에 대해서 redshift 테이블 데이터 갱신
    """
    redshift_hook = RedshiftSQLHook(redshift_conn_id=cfg.REDSHIFT_CONN_ID)
    # 테이블 존재 여부 확인
    check_table_sql = f"""
    SELECT 1 FROM information_schema.tables WHERE table_name = '{red_table_name}';
    """
    check_result = redshift_hook.get_records(check_table_sql)
    logging.info(check_result)
    if check_result:
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
    FROM 's3://{cfg.AGG_BUCKET_PREFIX}/{agg_table_name}/'
    IAM_ROLE 'arn:aws:iam::{cfg.ACCOUNT_ID}:role/{cfg.REDSHIFT_COPY_ROLE_NAME}'
    FORMAT AS PARQUET;
    """
    logging.info(copy_query)
    redshift_hook.run(copy_query)


# def execute_redshift_query(file_name):
#     """
#     S3에서 SQL 파일을 읽고 redshift로 실행
#     """
#     # S3
#     query_key = f"{cfg.REDSHIFT_QUERY_PREFIX}/{file_name}.sql"
#     s3 = boto3.client("s3")
#     response = s3.get_object(Bucket=cfg.REDSHIFT_QUERY_BUCKET, Key=query_key)
#     query = response["Body"].read().decode("utf-8")
#     logging.info(query)
#     # redshift
#     redshift_hook = RedshiftSQLHook(redshift_conn_id=cfg.REDSHIFT_CONN_ID)
#     redshift_hook.get_records(query)
#     # for row in results:
#     #     print(row)


# def execute_redshift_query(file_name="create_copy"):
#     """
#     S3에서 SQL 파일을 읽고, 여러 SQL 명령문을 순차적으로 Redshift에 실행.
#     """
#     # S3에서 SQL 파일 읽기
#     query_key = f"{cfg.REDSHIFT_QUERY_PREFIX}/{file_name}.sql"
#     s3 = boto3.client("s3")
#     response = s3.get_object(Bucket=cfg.REDSHIFT_QUERY_BUCKET, Key=query_key)
#     sql_content = response["Body"].read().decode("utf-8")
#     logging.info("SQL file content loaded.")

#     # SQL 명령문을 ";" 기준으로 분리
#     queries = [query.strip() for query in sql_content.split(";") if query.strip()]
#     logging.info(f"Found {len(queries)} SQL statements.")

#     # Redshift 연결
#     redshift_hook = RedshiftSQLHook(redshift_conn_id=cfg.REDSHIFT_CONN_ID)

#     # SQL 명령문 순차 실행
#     for i, query in enumerate(queries, 1):
#         logging.info(f"Executing query {i}/{len(queries)}: {query}")
#         try:
#             redshift_hook.run(query)
#             logging.info(f"Query {i} executed successfully.")
#         except Exception as e:
#             logging.error(f"Error executing query {i}: {e}")
#             raise


# def select_table():
#     return PythonOperator(
#         task_id='execute_redshift_query',
#         python_callable=execute_redshift_query,
#         op_kwargs={"file_name": f'create_table_{table_name}'},
#     )


# def select_redshift_query():
#     """
#     Connects to Redshift and executes a query.
#     """
#     redshift_hook = RedshiftSQLHook(redshift_conn_id=cfg.REDSHIFT_CONN_ID)
#     sql_query = f"""
#     SELECT entity_name FROM test_analysis_results LIMIT 10;
#     """
#     results = redshift_hook.get_records(sql_query)
#     for row in results:
#         logging.info(row)


# def create_table(table_name, retry=3, retry_delay=10):
#     return PythonOperator(
#         task_id=f"create_table_{table_name}",
#         python_callable=execute_redshift_query,
#         op_kwargs={
#             "file_name": f"create_table_{table_name}",
#         },
#         # retries=retry,
#         # retry_delay=timedelta(seconds=retry_delay),
#         # on_retry_callback=send_slack_message_fail,
#         # on_failure_callback=send_slack_message_fail,
#         # on_success_callback=send_slack_message_success,
#     )
