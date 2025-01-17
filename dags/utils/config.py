import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable

SEOUL_KEY = Variable.get("DATA_SEOUL_API_KEY")
LIBRARY_KEY = Variable.get("DATA4LIBRARY_API_KEY")
DISTRICTS_KOR_ENG = json.loads(Variable.get("DISTRICTS_KOR_ENG"))

LAST_MONTH_DATE = datetime.now().date() - relativedelta(months=1)
LAST_MONTH = (datetime.now().date() - relativedelta(months=1)).strftime("%Y%m")

ATHENA_QUERY_BUCKET = "cloudtree-mwaa-query"
ATHENA_QUERY_PREFIX = "athena-query"
ATHENA_QUERY_OUTPUT_BUCKET = "cloudtree-athena-query-result"
ATHENA_QUERY_OUTPUT_PREFIX = "mwaa-dag-query-results"

RAW_DB = 'cloudtree_raw_db'
TRANS_DB = 'cloudtree_transformed_db'
AGG_DB = 'cloudtree_aggregated_db'

AGG_TABLE = ['library_culture_analysis']

"""
bll: 인기대출도서
cul: 서울문화행사
lib: 도서관 정보
bus: 버스정류장
sta: 지하철역
park: 도시공원
"""

BLL_DICT = {
    'raw_db_nm': 'cloudtree_raw_db',
    'trans_db_nm': 'cloudtree_transformed_db',
    'raw_table_nm': 'best_loan_list_raw',
    'trans_table_nm': 'best_loan_list',
    'trans_job_nm': 'best_loan_list_job',
    'api_lambda_nm': 'getBestLoanAPI',
    'lambda_payload': {
        "districts": list(DISTRICTS_KOR_ENG.keys()),
        "key": LIBRARY_KEY,
        "periodStart": LAST_MONTH,
        "periodEnd": LAST_MONTH,
    },
}

CUL_DICT = {
    'raw_db_nm': 'cloudtree_raw_db',
    'trans_db_nm': 'cloudtree_transformed_db',
    'raw_parse_table_nm': 'cultural_event_parse_raw',
    'raw_info_table_nm': 'cultural_event_info_raw',
    'trans_table_nm': 'cultural_event_info',
    'keyword_job_nm': 'cultural_event_keyword_job',
    'trans_job_nm': 'best_loan_list_job',
    'api_lambda_nm': 'getCulEventAPI',
    'lambda_payload': {
        "districts": list(DISTRICTS_KOR_ENG.keys()),
        "key": SEOUL_KEY,
    },
}

LIB_DICT = {
    'raw_db_nm': 'cloudtree_raw_db',
    'trans_db_nm': 'cloudtree_transformed_db',
    'raw_table_nm': 'library_data_raw',
    'trans_table_nm': 'library_data',
    'trans_job_nm': 'library_data_job',
    'api_lambda_nm': 'getLibDataAPI',
    'lambda_payload': {"districts": list(DISTRICTS_KOR_ENG.keys()), "key": LIBRARY_KEY},
}

BUS_DICT = {
    'raw_db_nm': 'cloudtree_raw_db',
    'trans_db_nm': 'cloudtree_transformed_db',
    'raw_table_nm': 'bus_stop_loc_raw',
    'trans_table_nm': 'bus_stop_loc',
    'trans_job_nm': 'bus_stop_loc_job',
    'api_lambda_nm': 'getBusStopLocAPI',
    'lambda_payload': {"key": SEOUL_KEY},
}

STA_DICT = {
    'raw_db_nm': 'cloudtree_raw_db',
    'trans_db_nm': 'cloudtree_transformed_db',
    'raw_table_nm': 'subway_station_loc_raw',
    'trans_table_nm': 'subway_station_loc',
    'trans_job_nm': 'subway_station_loc_job',
    'api_lambda_nm': 'getSubwayStationLocAPI',
    'lambda_payload': {"key": SEOUL_KEY},
}

PARK_DICT = {
    'raw_db_nm': 'cloudtree_raw_db',
    'trans_db_nm': 'cloudtree_transformed_db',
    'raw_table_nm': 'city_park_info_raw',
    'trans_table_nm': 'city_park_info',
    'trans_job_nm': 'city_park_info_job',
}
