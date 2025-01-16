import json
import boto3
import requests
import logging
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

"""
taskgroup, task, dag 의존성 test code 1
"""


def task_alert(task_id, parm1=0, parm2=0, parm3=0, parm4=0):
    return BashOperator(
        task_id=f'task_{task_id}',
        bash_command=f'echo "tasks completed! (task: {task_id})"',
    )


# ==================================================================== DAG ====================================================================

with DAG(
    dag_id="task-flow-test-dag-1",
    start_date=days_ago(1),
    # schedule_interval='10 15 1 * *', # 매달 1일 오전 12시 10분
    schedule_interval=None,
    catchup=False,
) as dag:

    seoul_key = Variable.get("DATA_SEOUL_API_KEY")
    library_key = Variable.get("DATA4LIBRARY_API_KEY")
    districts_kor_eng = json.loads(Variable.get("DISTRICTS_KOR_ENG"))

    last_month_date = datetime.now().date() - relativedelta(months=1)
    last_month = (datetime.now().date() - relativedelta(months=1)).strftime("%Y%m")

    """
    bll: 인기대출도서
    cul: 서울문화행사
    """

    bll_dict = {
        'raw_db_nm': 'cloudtree_raw_db',
        'trans_db_nm': 'cloudtree_transformed_db',
        'raw_table_nm': 'best_loan_list_raw',
        'trans_table_nm': 'best_loan_list',
        'trans_job_nm': 'best_loan_list_job',
        'api_lambda_nm': 'getBestLoanAPI',
        'lambda_payload': {
            "districts": list(districts_kor_eng.keys()),
            "key": library_key,
            "periodStart": last_month,
            "periodEnd": last_month,
        },
    }

    cul_dict = {
        'raw_db_nm': 'cloudtree_raw_db',
        'trans_db_nm': 'cloudtree_transformed_db',
        'raw_parse_table_nm': 'cultural_event_parse_raw',
        'raw_info_table_nm': 'cultural_event_info_raw',
        'trans_table_nm': 'cultural_event_info',
        'keyword_job_nm': 'cultural_event_keyword_job',
        'trans_job_nm': 'best_loan_list_job',
        'api_lambda_nm': 'getCulEventAPI',
        'lambda_payload': {"districts": list(districts_kor_eng.keys()), "key": seoul_key},
    }

    # ============================ bll: create table ============================

    with TaskGroup(group_id='group1') as group1:
        ct_sql_file_nm = f"create_table_{bll_dict['raw_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = task_alert('fetch_s3_task', fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = task_alert('create_table_task', fetch_s3_task_id, bll_dict['raw_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    with TaskGroup(group_id='group2') as group2:
        ct_sql_file_nm = f"create_table_{bll_dict['trans_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = task_alert('fetch_s3_task', fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = task_alert('create_table_task', fetch_s3_task_id, bll_dict['trans_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    # ============================ bll: load to L2 ============================

    with TaskGroup(group_id='group3') as group3:
        invoke_api_lambda = task_alert('invoke_api_lambda', bll_dict['api_lambda_nm'], bll_dict['lambda_payload'])
        msck_raw = task_alert('msck_raw', bll_dict['raw_db_nm'], bll_dict['raw_table_nm'])
        run_trans_job = task_alert('run_trans_job', bll_dict['trans_job_nm'])
        msck_trans = task_alert('msck_trans', bll_dict['trans_db_nm'], bll_dict['trans_table_nm'])

        invoke_api_lambda >> msck_raw >> run_trans_job >> msck_trans

    # ============================ cul: create table ============================

    with TaskGroup(group_id='group4') as group4:
        ct_sql_file_nm = f"create_table_{cul_dict['raw_parse_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = task_alert('fetch_s3_task', fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = task_alert('create_table_task', fetch_s3_task_id, cul_dict['raw_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    with TaskGroup(group_id='group7') as group7:
        ct_sql_file_nm = f"create_table_{cul_dict['raw_info_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = task_alert('fetch_s3_task', fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = task_alert('create_table_task', fetch_s3_task_id, cul_dict['raw_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    with TaskGroup(group_id='group5') as group5:
        ct_sql_file_nm = f"create_table_{cul_dict['trans_table_nm']}"
        fetch_s3_task_id = f"fetch_sql_{ct_sql_file_nm}"
        fetch_s3_task = task_alert('fetch_s3_task', fetch_s3_task_id, ct_sql_file_nm)
        create_table_task = task_alert('create_table_task', fetch_s3_task_id, cul_dict['trans_db_nm'])
        # TaskGroup 의존성 설정
        fetch_s3_task >> create_table_task

    # ============================ cul: load to L2 ============================

    with TaskGroup(group_id='group6') as group6:
        invoke_api_lambda = task_alert('invoke_api_lambda', cul_dict['api_lambda_nm'], cul_dict['lambda_payload'])
        msck_parse_raw = task_alert('msck_parse_raw', cul_dict['raw_db_nm'], cul_dict['raw_parse_table_nm'])
        run_keyword_job = task_alert('run_keyword_job', cul_dict['keyword_job_nm'])
        msck_info_raw = task_alert('msck_info_raw', cul_dict['raw_db_nm'], cul_dict['raw_info_table_nm'])
        run_trans_job = task_alert('run_trans_job', cul_dict['trans_job_nm'])
        msck_trans = task_alert('msck_trans', cul_dict['trans_db_nm'], cul_dict['trans_table_nm'])

        invoke_api_lambda >> msck_parse_raw >> run_keyword_job >> msck_info_raw >> run_trans_job >> msck_trans

    task10 = DummyOperator(task_id='task10')

    # Trigger DAG2
    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_dag2",
        trigger_dag_id="task-flow-test-dag-2",  # 트리거할 DAG의 ID
        wait_for_completion=False,  # True로 설정하면 DAG2 완료를 기다림
    )

    # task group 의존성 설정

    (group1, group2) >> group3
    (group4, group7, group5) >> group6
    [group3, group6] >> task10 >> trigger_dag2
    """ DAG graph
    group1    group2          group4    group7    group5
       \      /                   \      |      /
        group3                     group6
               \                  /
                \                /
                 \              /
                  \            /
                   \          /
                    \        /
                     task10
                       |
                       v
                 trigger_dag2
    
    """
