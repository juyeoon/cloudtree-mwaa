from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import time
import requests
import pandas as pd
import boto3
import xml.etree.ElementTree as ET
from io import StringIO


def lambda_handler(event, context):

    bucket_name = 'cloudtree-raw-data'
    main_prefix = 'best-loan-list'

    base_api_url = "http://data4library.kr/api/loanItemSrch"
    authKey = event.get('key')

    # api 파라미터
    period_start = datetime.strptime(event.get('periodStart'), "%Y%m").date()
    period_end = datetime.strptime(event.get('periodEnd'), "%Y%m").date()

    region = 11
    dtl_gu_list = event.get('districts')
    pageSize = 200

    request_date = datetime.now().date().strftime("%y%m%d")

    districts_dict = {
        '강남구': 11230,
        '강동구': 11250,
        '강북구': 11090,
        '강서구': 11160,
        '관악구': 11210,
        '광진구': 11050,
        '구로구': 11170,
        '금천구': 11180,
        '노원구': 11110,
        '도봉구': 11100,
        '동대문구': 11060,
        '동작구': 11200,
        '마포구': 11140,
        '서대문구': 11130,
        '서초구': 11220,
        '성동구': 11040,
        '성북구': 11080,
        '송파구': 11240,
        '양천구': 11150,
        '영등포구': 11190,
        '용산구': 11030,
        '은평구': 11120,
        '종로구': 11010,
        '중구': 11020,
        '중랑구': 11070,
    }

    current_start_date = period_start
    while current_start_date <= period_end:
        endDt = current_start_date + relativedelta(months=1) - timedelta(days=1)

        print(current_start_date)
        print(endDt)

        for dtl_gu in dtl_gu_list:
            dtl_region = districts_dict[dtl_gu]

            params = {
                'authKey': authKey,
                'startDt': current_start_date.strftime('%Y-%m-%d'),
                'endDt': endDt.strftime('%Y-%m-%d'),
                'region': region,
                'dtl_region': dtl_region,
                'pageSize': pageSize,
            }

            df = pd.DataFrame()

            print("====================\napi 호출 시작")

            time.sleep(2)
            response = requests.get(base_api_url, params=params)
            if response.status_code == 200:
                print("api 호출 성공: 200")
                df = parse_xml_data(response.text)
            else:
                print(f"api 호출 실패: {response.status_code}")
                exit()

            year = current_start_date.year
            month = current_start_date.month

            df = transform_dataframe(df, dtl_gu, year, month)

            file_name = f'bestLoanList_{dtl_gu}_{year}{month:02}_{pageSize}'  #  =========== custom value ===========
            file_path = f'{main_prefix}/district={dtl_gu}/year={year}/month={month}/{file_name}'  #  =========== custom value ===========

            save_to_csv(df, bucket_name, f'{file_path}.csv')

        current_start_date += relativedelta(months=1)

    print("------------전체 api 파싱 끝------------")


def parse_xml_data(xml_data):
    '''
    xml 파싱 및 태그 추출
    '''
    root = ET.fromstring(xml_data)
    data = []
    for doc in root.findall('.//doc'):
        row = {}
        for child in doc:
            tag_name = child.tag
            tag_value = child.text.strip() if child.text else ''
            row[tag_name] = tag_value
        data.append(row)
    return pd.DataFrame(data)


def transform_dataframe(df, dtl_gu, std_year, std_month):
    '''
    df 컬럼 변형
    '''
    ret_df = df
    ret_df[['class_major', 'class_sub', 'class_detailed']] = ret_df['class_nm'].str.split(' > ', expand=True)
    ret_df = ret_df.drop(columns=['class_nm'])
    ret_df['district'] = dtl_gu
    ret_df['year'] = std_year
    ret_df['month'] = std_month
    return ret_df


def save_to_csv(df, bucket, file_key):
    """
    df를 csv 파일로 변형하여 s3에 업로드
    """
    s3 = boto3.client("s3")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    try:
        s3.put_object(
            Bucket=bucket,
            Key=file_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv; charset=utf-8",
            ContentEncoding="utf-8",
        )
        print(f"File successfully uploaded to s3://{bucket}/{file_key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
