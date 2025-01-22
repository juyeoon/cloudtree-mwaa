from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import config as cfg
from utils import dag_notifications as dn
from utils import lambda_helpers as lh
from utils import athena_helpers as ath
from utils import glue_helpers as gh
from utils import s3_helpers as s3h

with DAG(
    dag_id="_cycleLoading_oneMonth_L1_bll",
    description="1개월 주기 적재: 인기대출도서 목록(bll), L1까지 ETL",
    start_date=days_ago(1),
    schedule_interval='10 0 * * *',
    catchup=False,
) as dag:

    start_task = dn.task_alert_dag_state(dag.dag_id, "start")

    bll_dict = cfg.BLL_DICT

    with TaskGroup(group_id="etl_bll") as etl_bll:
        bll_data_year = cfg.LAST_MONTH_DATE.year
        bll_data_month = cfg.LAST_MONTH_DATE.month
        invoke_api_lambda = s3h.for_demo_task(list(cfg.DISTRICTS_KOR_ENG.keys()), bll_data_year, bll_data_month)
        msck_raw = ath.task_msck_repair_table(bll_dict["raw_db_nm"], bll_dict["raw_table_nm"])
        run_trans_job = gh.task_run_glue_job(bll_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(bll_dict["trans_db_nm"], bll_dict["trans_table_nm"])
        # TaskGroup 의존성 설정
        invoke_api_lambda >> msck_raw >> run_trans_job >> msck_trans

    end_task = dn.task_alert_dag_state(dag.dag_id, "end")

    trigger_next_ETL = TriggerDagRunOperator(
        task_id=f'trigger_next_ETL_from_{dag.dag_id}',
        trigger_dag_id='_update_L2_analysis_bll',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    start_task >> etl_bll >> end_task >> trigger_next_ETL
    # start_task >> etl_bll >> end_task
