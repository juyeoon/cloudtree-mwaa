from airflow.operators.bash import BashOperator

BLL_SOURCE_BUCKET = "cloudtree-best-loan-raw"
BLL_SOURCE_BUCKET_PREFIX = "best-loan-list"
BLL_DESTINATION_BUCKET = "cloudtree-raw-data"
BLL_DESTINATION_BUCKET_PREFIX = "best-loan-list"


def copy_bll_raw_data(district_eng, district_kor, year, month):
    return BashOperator(
        task_id=f'copy_data_{district_eng}_{year}_{month}',
        bash_command=f'aws s3 cp s3://{BLL_SOURCE_BUCKET}/{SOURCE_BUCKET_PREFIX}/district={district_kor}/year={year}/month={month}/ s3://{BLL_DESTINATION_BUCKET}/{BLL_DESTINATION_BUCKET_PREFIX}/district={district_kor}/year={year}/month={month}/ --recursive',
    )
