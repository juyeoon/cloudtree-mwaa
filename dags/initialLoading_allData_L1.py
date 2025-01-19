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
    dag_id="initialLoading_allData_L1",
    description="모든 배치 데이터(bll, cul, lib, bus, sta, park) 초기 적재, L1까지 ETL",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start_task = dn.task_alert_dag_state(dag.dag_id, "start")

    bll_dict = cfg.BLL_DICT
    cul_dict = cfg.CUL_DICT
    lib_dict = cfg.LIB_DICT
    bus_dict = cfg.BUS_DICT
    sta_dict = cfg.STA_DICT
    park_dict = cfg.PARK_DICT

    delete_trans_data = s3h.task_delete_transformed_data(cfg.TRANS_PREFIX)

    with TaskGroup(group_id="etl_bll") as etl_bll:
        msck_raw = ath.task_msck_repair_table(
            bll_dict["raw_db_nm"], bll_dict["raw_table_nm"]
        )
        reset_bookmark = gh.task_delete_glue_job_bookmark(bll_dict["trans_job_nm"])
        run_trans_job = gh.task_run_glue_job(bll_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            bll_dict["trans_db_nm"], bll_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        msck_raw >> reset_bookmark >> run_trans_job >> msck_trans

    with TaskGroup(group_id="etl_cul") as etl_cul:
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
            msck_parse_raw
            >> run_keyword_job
            >> msck_info_raw
            >> run_trans_job
            >> msck_trans
        )

    with TaskGroup(group_id="etl_lib") as etl_lib:
        msck_raw = ath.task_msck_repair_table(
            lib_dict["raw_db_nm"], lib_dict["raw_table_nm"]
        )
        run_trans_job = gh.task_run_glue_job(lib_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            lib_dict["trans_db_nm"], lib_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        msck_raw >> run_trans_job >> msck_trans

    with TaskGroup(group_id="etl_bus") as etl_bus:
        msck_raw = ath.task_msck_repair_table(
            bus_dict["raw_db_nm"], bus_dict["raw_table_nm"]
        )
        run_trans_job = gh.task_run_glue_job(bus_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            bus_dict["trans_db_nm"], bus_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        msck_raw >> run_trans_job >> msck_trans

    with TaskGroup(group_id="etl_sta") as etl_sta:
        msck_raw = ath.task_msck_repair_table(
            sta_dict["raw_db_nm"], sta_dict["raw_table_nm"]
        )
        run_trans_job = gh.task_run_glue_job(sta_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            sta_dict["trans_db_nm"], sta_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        msck_raw >> run_trans_job >> msck_trans

    with TaskGroup(group_id="etl_park") as etl_park:
        msck_raw = ath.task_msck_repair_table(
            park_dict["raw_db_nm"], park_dict["raw_table_nm"]
        )
        run_trans_job = gh.task_run_glue_job(park_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            park_dict["trans_db_nm"], park_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        msck_raw >> run_trans_job >> msck_trans

    end_task = dn.task_alert_dag_state(dag.dag_id, "end")

    (
        start_task
        >> delete_trans_data
        >> [etl_bll, etl_cul, etl_lib, etl_bus, etl_sta, etl_park]
        >> end_task
    )
