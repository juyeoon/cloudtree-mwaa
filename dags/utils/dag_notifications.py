from airflow.operators.python import PythonOperator
from utils import slack_helpers as sh


def task_alert_dag_state(dag_id, type):
    """
    task: dag 상태 slack 알림
    """
    return PythonOperator(
        task_id=f"{type}_{dag_id}",
        python_callable=sh.send_dag_state_message,
        provide_context=True,
        op_kwargs={
            "type": f"{type}",
        },
    )
