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


def for_demo_copy_data_to_s3(districts, year, month):
    """
    S3 버킷 간 데이터를 복사하는 함수 (boto3 사용)
    """
    s3 = boto3.client('s3')

    bll_source_bucket = "cloudtree-best-loan-raw"
    bll_source_bucket_prefix = "best-loan-list"
    bll_destination_bucket = "cloudtree-raw-data"
    bll_destination_bucket_prefix = "best-loan-list"

    for district_kor in districts:
        source_prefix = f"{bll_source_bucket_prefix}/district={district_kor}/year={year}/month={month}/"

        # 객체 목록 가져오기
        response = s3.list_objects_v2(Bucket=bll_source_bucket, Prefix=source_prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                source_key = obj['Key']
                destination_key = source_key.replace(bll_source_bucket_prefix, bll_destination_bucket_prefix)

                # 객체 복사
                copy_source = {'Bucket': bll_source_bucket, 'Key': source_key}
                s3.copy_object(CopySource=copy_source, Bucket=bll_destination_bucket, Key=destination_key)


def for_demo_task(districts, year, month):
    """
    test용 task: 인기대출도서 데이터를 원본 데이터 버킷에서 raw 버킷으로 복사
    """
    return PythonOperator(
        task_id=f"invoke_lambda_getBestLoanAPI",
        python_callable=for_demo_copy_data_to_s3,
        op_args=[districts, year, month],
        on_failure_callback=sh.send_slack_message_fail,
        on_success_callback=sh.send_slack_message_success,
    )
