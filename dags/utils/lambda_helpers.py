import json
from datetime import timedelta
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from utils import slack_helpers as sh


def task_invoke_lambda(func_name, payload=None, retry=3, retry_delay=1):
    """
    task: AWS Lambda 호출
    """
    return LambdaInvokeFunctionOperator(
        task_id=f"invoke_lambda_{func_name}",
        function_name=func_name,
        payload=json.dumps(payload),
        retries=retry,
        retry_delay=timedelta(seconds=retry_delay),
        on_retry_callback=sh.send_slack_message_fail,
        on_failure_callback=sh.send_slack_message_fail,
        on_success_callback=sh.send_slack_message_success,
    )
