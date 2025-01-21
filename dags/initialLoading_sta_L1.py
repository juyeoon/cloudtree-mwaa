from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import config as cfg
from utils import dag_notifications as dn
from utils import athena_helpers as ath
from utils import glue_helpers as gh
from utils import s3_helpers as s3h

with DAG(
    dag_id="initialLoading_sta_L1",
    description="지하철역(sta) 재적재. L1까지 ETL",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start_task = dn.task_alert_dag_state(dag.dag_id, "start")

    sta_dict = cfg.STA_DICT

    delete_trans_data = s3h.task_delete_transformed_data([cfg.TRANS_PREFIX['sta']])

    with TaskGroup(group_id="etl_sta") as etl_sta:
        msck_raw = ath.task_msck_repair_table(sta_dict["raw_db_nm"], sta_dict["raw_table_nm"])
        run_trans_job = gh.task_run_glue_job(sta_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(sta_dict["trans_db_nm"], sta_dict["trans_table_nm"])
        # TaskGroup 의존성 설정
        msck_raw >> run_trans_job >> msck_trans

    end_task = dn.task_alert_dag_state(dag.dag_id, "end")

    trigger_next_ETL = TriggerDagRunOperator(
        task_id=f'trigger_next_ETL_from_{dag.dag_id}',
        trigger_dag_id='update_L2_analysis',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    (start_task >> delete_trans_data >> etl_sta >> end_task >> trigger_next_ETL)
