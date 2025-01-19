import boto3
import logging
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils import config as cfg
from utils import slack_helpers as sh


def delete_s3_objects(bucket_name, prefixs):
    """
    S3 버킷의 특정 prefix 아래의 모든 객체를 삭제하는 함수
    """
    s3 = boto3.resource("s3")  # 환경에 구성된 자격 증명을 사용
    bucket = s3.Bucket(bucket_name)

    for prefix in prefixs:
        # 객체 삭제
        prefix = f"{prefix}/"
        objects_to_delete = [
            {"Key": obj.key} for obj in bucket.objects.filter(Prefix=prefix)
        ]
        if objects_to_delete:
            bucket.delete_objects(Delete={"Objects": objects_to_delete})
            logging.info(
                f"Deleted {len(objects_to_delete)} objects from {bucket_name}/{prefix}"
            )
        else:
            logging.info(f"No objects found in {bucket_name}/{prefix}")


def task_delete_transformed_data(prefixs, retry=3, retry_delay=5):
    """
    task: transformed bucket의 prefix에 해당하는 object 삭제
    """
    return PythonOperator(
        task_id="delete_transformed_s3_objects",
        python_callable=delete_s3_objects,
        op_kwargs={
            "bucket_name": cfg.TRANS_BUCKET,
            "prefixs": prefixs,
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
