from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from utils.slack_helpers import send_slack_message_fail, send_slack_message_success
from datetime import timedelta


def run_glue_job(job_name, retry=3, retry_delay=10):
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
