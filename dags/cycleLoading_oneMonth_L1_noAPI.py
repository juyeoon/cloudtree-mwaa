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
    dag_id="cycleLoading_oneMonth_L1_noAPI",
    description="1개월 주기 적재: 인기대출도서 목록(bll), 서울시 문화행사 목록(cul), L1까지 ETL",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start_task = dn.task_alert_dag_state(dag.dag_id, "start")

    bll_dict = cfg.BLL_DICT
    cul_dict = cfg.CUL_DICT

    with TaskGroup(group_id="etl_bll") as etl_bll:
        bll_data_year = cfg.LAST_MONTH_DATE.year
        bll_data_month = cfg.LAST_MONTH_DATE.month
        copy_last_month_data = []
        for district_kor, district_eng in cfg.DISTRICTS_KOR_ENG.items():
            copy_task = s3h.task_copy_bll_raw_data(
                district_eng, district_kor, bll_data_year, bll_data_month
            )
            copy_last_month_data.append(copy_task)
        msck_raw = ath.task_msck_repair_table(
            bll_dict["raw_db_nm"], bll_dict["raw_table_nm"]
        )
        run_trans_job = gh.task_run_glue_job(bll_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            bll_dict["trans_db_nm"], bll_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        copy_last_month_data >> msck_raw >> run_trans_job >> msck_trans

    with TaskGroup(group_id="etl_cul") as etl_cul:
        invoke_api_lambda = lh.task_invoke_lambda(
            cul_dict["api_lambda_nm"], cul_dict["lambda_payload"]
        )
        msck_parse_raw = ath.task_msck_repair_table(
            cul_dict["raw_db_nm"], cul_dict["raw_parse_table_nm"]
        )
        run_keyword_job = gh.task_run_glue_job(cul_dict["keyword_job_nm"])
        msck_info_raw = ath.task_msck_repair_table(
            cul_dict["raw_db_nm"], cul_dict["raw_info_table_nm"]
        )
        run_trans_job = gh.task_run_glue_job(cul_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            cul_dict["trans_db_nm"], cul_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        (
            invoke_api_lambda
            >> msck_parse_raw
            >> run_keyword_job
            >> msck_info_raw
            >> run_trans_job
            >> msck_trans
        )

    end_task = dn.task_alert_dag_state(dag.dag_id, "end")

    start_task >> [etl_bll, etl_cul] >> end_task
