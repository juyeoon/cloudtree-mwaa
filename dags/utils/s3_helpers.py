import boto3
from botocore.config import Config
import logging
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils import config as cfg
from utils import slack_helpers as sh


def delete_s3_objects(bucket_name, prefixes):
    """
    S3 버킷의 특정 prefix 아래의 모든 객체를 삭제하는 함수
    """
    # 재시도 설정
    s3_config = Config(retries={'max_attempts': 2, 'mode': 'adaptive'})  # 적응형 재시도 모드 (속도 제한)

    # S3 클라이언트 생성
    s3_client = boto3.client("s3", config=s3_config)
    for prefix in prefixes:
        prefix = f"{prefix}/"
        logging.info(f"Searching for objects in {bucket_name}/{prefix}")

        # S3 객체 나열
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" in response:
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]

            # 객체 삭제
            delete_response = s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects_to_delete})
            logging.info(f"Deleted {len(objects_to_delete)} objects from {bucket_name}/{prefix}")
        else:
            logging.info(f"No objects found in {bucket_name}/{prefix}")


def task_delete_transformed_data(prefixes, retry=3, retry_delay=5):
    """
    task: transformed bucket의 prefix에 해당하는 object 삭제
    """
    return PythonOperator(
        task_id="delete_transformed_s3_objects",
        python_callable=delete_s3_objects,
        op_kwargs={
            "bucket_name": cfg.TRANS_BUCKET,
            "prefixes": prefixes,
        },
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=sh.send_slack_message_fail,
        on_failure_callback=sh.send_slack_message_fail,
        on_success_callback=sh.send_slack_message_success,
    )


def task_delete_agg_data(prefixes, retry=3, retry_delay=5):
    """
    task: aggregated bucket의 prefix에 해당하는 object 삭제
    """
    return PythonOperator(
        task_id="delete_agg_s3_objects",
        python_callable=delete_s3_objects,
        op_kwargs={
            "bucket_name": cfg.AGG_BUCKET,
            "prefixes": prefixes,
        },
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=sh.send_slack_message_fail,
        on_failure_callback=sh.send_slack_message_fail,
        on_success_callback=sh.send_slack_message_success,
    )


def task_copy_bll_raw_data(district_eng, district_kor, year, month):
    """
    test용 task: 인기대출도서 데이터를 원본 데이터 버킷에서 raw 버킷으로 복사
    """
    bll_source_bucket = "cloudtree-best-loan-raw"
    bll_source_bucket_prefix = "best-loan-list"
    bll_destination_bucket = "cloudtree-raw-data"
    bll_destination_bucket_prefix = "best-loan-list"
    return BashOperator(
        task_id=f"copy_data_{district_eng}_{year}_{month}",
        bash_command=f"aws s3 cp s3://{bll_source_bucket}/{bll_source_bucket_prefix}/district={district_kor}/year={year}/month={month}/ s3://{bll_destination_bucket}/{bll_destination_bucket_prefix}/district={district_kor}/year={year}/month={month}/ --recursive",
    )
