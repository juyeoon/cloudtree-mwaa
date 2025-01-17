from airflow.operators.python import PythonOperator
from utils.slack_helpers import send_dag_state_message


def alert_dag_state(dag_id, type):
    """
    dag 상태 slack 알람
    """
    return PythonOperator(
        task_id=f'{dag_id}_{type}_task',
        python_callable=send_dag_state_message,
        provide_context=True,
        op_kwargs={
            'type': f'{type}',
        },
    )
