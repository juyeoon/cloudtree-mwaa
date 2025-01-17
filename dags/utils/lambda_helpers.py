import json
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from datetime import timedelta
from utils.slack_helpers import send_slack_message_fail, send_slack_message_success


def invoke_lambda(func_name, payload=None, retry=3, retry_delay=1):
    """
    AWS Lambda 호출
    """
    return LambdaInvokeFunctionOperator(
        task_id=f"invoke_lambda_{func_name}",
        function_name=func_name,
        payload=json.dumps(payload),
        retries=retry,
        retry_delay=timedelta(minutes=retry_delay),
        on_retry_callback=send_slack_message_fail,
        on_failure_callback=send_slack_message_fail,
        on_success_callback=send_slack_message_success,
    )
