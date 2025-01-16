import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


def delete_last_month_data(eng, district, year, month):
    return BashOperator(
        task_id=f'delete_data_{eng}_{year}_{month}',
        bash_command=f'aws s3 rm s3://cloudtree-raw-data/best-loan-list/district={district}/year={year}/month={month}/ --recursive',
    )


def update_data_catalog(database_name, table_name):
    """
    data catalog 갱신
    """
    return AthenaOperator(
        task_id=f"msck_repair_{table_name}",
        query=f"MSCK REPAIR TABLE {table_name}",
        database=database_name,
        output_location="s3://cloudtree-athena-query-result/mwaa-dag-query-results/",
    )


# DAG 정의
with DAG(
    dag_id="delete-best-loan-raw-specific_month",
    description='Delete best loan list raw data for a specific month in all districts',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    years = [2024]  # setting value
    months = [9, 10, 11, 12]  # setting value

    districts_english = json.loads(Variable.get("DISTRICTS_ENGLISH"))

    previous_month_group = None

    for year in years:
        for month in months:
            with TaskGroup(group_id=f'month_{month}_tasks') as month_group:
                for district_kor, district_eng in districts_english.items():
                    delete_last_month_data(district_eng, district_kor, year, month)

            if previous_month_group:
                previous_month_group >> month_group
            previous_month_group = month_group

    final_task = BashOperator(
        task_id='final_task',
        bash_command='echo "All parallel tasks completed!"',
    )

    msck_best_loan_raw = update_data_catalog("cloudtree_raw_db", "best_loan_list_raw")  #
    msck_best_loan_trans = update_data_catalog("cloudtree_transformed_db", "best_loan_list")  #

    # task 의존성 설정

    previous_month_group >> msck_best_loan_raw >> msck_best_loan_trans >> final_task
