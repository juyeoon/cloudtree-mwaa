from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import config as cfg
from utils import dag_notifications as dn
from utils import lambda_helpers as lh
from utils import athena_helpers as ath
from utils import glue_helpers as gh

with DAG(
    dag_id="cycleLoading_sixMonths_L1",
    description="6개월 주기 적재: 도서관 정보(lib), 버스정류장(bus), 지하철역(sta), 도시공원(park), L1까지 ETL",
    start_date=days_ago(1),
    # schedule_interval='10 15 17 6,12 *', # 6월 18일, 12월 18일 오전 12시 10분
    schedule_interval=None,
    catchup=False,
) as dag:

    start_task = dn.task_alert_dag_state(dag.dag_id, "start")

    lib_dict = cfg.LIB_DICT
    bus_dict = cfg.BUS_DICT
    sta_dict = cfg.STA_DICT
    park_dict = cfg.PARK_DICT

    with TaskGroup(group_id="etl_lib") as etl_lib:
        invoke_api_lambda = lh.task_invoke_lambda(
            lib_dict["api_lambda_nm"], lib_dict["lambda_payload"]
        )
        msck_raw = ath.task_msck_repair_table(
            lib_dict["raw_db_nm"], lib_dict["raw_table_nm"]
        )
        run_trans_job = gh.task_run_glue_job(lib_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            lib_dict["trans_db_nm"], lib_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        invoke_api_lambda >> msck_raw >> run_trans_job >> msck_trans

    with TaskGroup(group_id="etl_bus") as etl_bus:
        invoke_api_lambda = lh.task_invoke_lambda(
            bus_dict["api_lambda_nm"], bus_dict["lambda_payload"]
        )
        msck_raw = ath.task_msck_repair_table(
            bus_dict["raw_db_nm"], bus_dict["raw_table_nm"]
        )
        run_trans_job = gh.task_run_glue_job(bus_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            bus_dict["trans_db_nm"], bus_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        invoke_api_lambda >> msck_raw >> run_trans_job >> msck_trans

    with TaskGroup(group_id="etl_sta") as etl_sta:
        invoke_api_lambda = lh.task_invoke_lambda(
            sta_dict["api_lambda_nm"], sta_dict["lambda_payload"]
        )
        msck_raw = ath.task_msck_repair_table(
            sta_dict["raw_db_nm"], sta_dict["raw_table_nm"]
        )
        run_trans_job = gh.task_run_glue_job(sta_dict["trans_job_nm"])
        msck_trans = ath.task_msck_repair_table(
            sta_dict["trans_db_nm"], sta_dict["trans_table_nm"]
        )
        # TaskGroup 의존성 설정
        invoke_api_lambda >> msck_raw >> run_trans_job >> msck_trans

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

    start_task >> [etl_lib, etl_bus, etl_sta, etl_park] >> end_task
