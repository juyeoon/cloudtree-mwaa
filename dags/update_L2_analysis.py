from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from utils import config as cfg
from utils import dag_notifications as dn
from utils import athena_helpers as ath
from utils import quicksight_helpers as qh
from utils import redshift_helpers as rh
from utils import s3_helpers as s3h

with DAG(
    dag_id="update_L2_analysis",
    description="L2 Bucket, Analysis(Redshift, Quicksight) 데이터 갱신",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start_task = dn.task_alert_dag_state(dag.dag_id, "start")

    agg_database_name = cfg.AGG_DB
    agg_tables = list(cfg.AGG_RED_TABLES_DICT.keys())

    # L2
    with TaskGroup(group_id="agg_etl") as agg_etl:
        for table_name in agg_tables:
            with TaskGroup(group_id=f"agg_etl_{table_name}") as agg_etl:
                delete_old_data = s3h.task_delete_agg_data(list(cfg.AGG_RED_TABLES_DICT.keys()))
                drop_table = ath.task_drop_table(agg_database_name, table_name)
                create_table = ath.task_create_table(agg_database_name, table_name)
                drop_table >> create_table

    # redshift
    with TaskGroup(group_id="update_redshift") as update_redshift:
        update_basic_analysis_table = rh.task_load_agg_data_to_red(cfg.AGG_RED_TABLES_DICT)
        update_advanced_analysis_table = rh.task_update_analysis_table(cfg.ADVANCED_ANALYSIS_TABLES)

        update_basic_analysis_table >> update_advanced_analysis_table

    # quicksight
    refresh_quicksight_spice = qh.task_refresh_spice()

    end_task = dn.task_alert_dag_state(dag.dag_id, "end")

    start_task >> agg_etl >> update_redshift >> refresh_quicksight_spice >> end_task
