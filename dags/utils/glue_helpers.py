import boto3
import logging
from datetime import timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from utils import slack_helpers as sh


def delete_glue_job_bookmark(job_name):
    """
    glue job의 bookmark를 reset하는 함수
    """
    glue_client = boto3.client("glue")
    response = glue_client.reset_job_bookmark(JobName=job_name)
    logging.info(f"Bookmark for job '{job_name}' has been reset successfully.")


def task_delete_glue_job_bookmark(job_name, retry=5, retry_delay=5):
    """
    task: glue job의 bookmark reset
    """
    return PythonOperator(
        task_id=f"reset_{job_name}_glue_bookmark",
        python_callable=delete_glue_job_bookmark,
        op_kwargs={"job_name": job_name},
    )


def task_run_glue_job(job_name, retry=3, retry_delay=10):
    """
    task: glue job 실행
    """
    return GlueJobOperator(
        task_id=f"run_glue_job_{job_name}",
        job_name=job_name,
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=sh.send_slack_message_fail,
        on_failure_callback=sh.send_slack_message_fail,
        on_success_callback=sh.send_slack_message_success,
    )
