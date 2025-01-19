import boto3
import uuid
import logging
from datetime import datetime, timedelta, timezone
from airflow.operators.python_operator import PythonOperator
from utils import slack_helpers as sh
from utils import config as cfg


def get_dataset_ids_by_names(datasets_names):
    """
    Quicksight의 데이터 세트 목록을 읽어 이름에 해당하는 dataset id 가져오는 함수
    """
    client = boto3.client("quicksight")
    dataset_ids = {}
    response = client.list_data_sets(AwsAccountId=cfg.ACCOUNT_ID)
    while response:
        for dataset in response.get("DataSetSummaries", []):
            dataset_name = dataset["Name"]
            dataset_id = dataset["DataSetId"]

            # 대상 이름 리스트에 포함된 데이터 세트만 저장
            if dataset_name in datasets_names:
                dataset_ids[dataset_name] = dataset_id

        next_token = response.get("NextToken")
        if next_token:
            response = client.list_data_sets(
                AwsAccountId=cfg.ACCOUNT_ID, NextToken=next_token
            )
        else:
            response = None
    return dataset_ids


def refresh_spice(dataset_names):
    """
    Quicksight의 데이터세트의 Spice를 refresh하는 함수
    """
    datasets_dict = get_dataset_ids_by_names(dataset_names)
    client = boto3.client("quicksight")
    for dataset_nm, dataset_id in datasets_dict.items():
        ingestion_id = f'ingestion-{datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")}-{uuid.uuid4()}'
        try:
            client.create_ingestion(
                DataSetId=dataset_id,
                AwsAccountId=cfg.ACCOUNT_ID,
                IngestionId=ingestion_id,
            )
            logging.info(
                f"Successfully refreshed Data Set: {dataset_nm} (ID: {dataset_id})"
            )
        except Exception as e:
            logging.error(
                f"Failed to refresh Data Set: {dataset_nm} (ID: {dataset_id}), Error: {str(e)}"
            )


def task_refresh_spice(retry=3, retry_delay=5):
    """
    task: Quicksight Spice를 refresh
    """
    return PythonOperator(
        task_id="refresh_spice",
        python_callable=refresh_spice,
        op_kwargs={
            "dataset_names": cfg.QUICKSIGHT_DATA_SET_NAMES,
        },
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=sh.send_slack_message_fail,
        on_failure_callback=sh.send_slack_message_fail,
        on_success_callback=sh.send_slack_message_success,
    )
