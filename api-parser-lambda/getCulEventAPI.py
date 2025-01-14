from datetime import datetime
import time
import requests
import pandas as pd
import boto3
import xml.etree.ElementTree as ET
from io import StringIO


def lambda_handler(event, context):

    bucket_name = 'cloudtree-raw-data'
    main_prefix = 'cultural-event-parse'

    base_api_url = "http://openapi.seoul.go.kr:8088"
    authKey = event.get('key')
    service = 'culturalEventInfo'

    # api 파라미터
    # districts: 자치구
    type = 'xml'
    start_index = 1
    page_size = 1000  # max 개수 1000개
    end_index = start_index + page_size - 1
    districts = event.get('districts')

    request_date = datetime.now().date().strftime("%y%m%d")

    df = pd.DataFrame()

    end_loop = False
    while not end_loop:
        request_url = f'{base_api_url}/{authKey}/{type}/{service}/{start_index}/{end_index}/'
        print("====================\napi 호출 시작")
        time.sleep(1)
        response = requests.get(request_url)
        if response.status_code == 200:
            print("api 호출 성공: 200")
            root = ET.fromstring(response.text)
            result_code_obj = root.find('.//RESULT/CODE')
            if result_code_obj is None:
                result_code_obj = root.find('.//CODE')
            result_code = result_code_obj.text
            if result_code == 'INFO-000':
                df = pd.concat([df, parse_xml_data(root)], ignore_index=True)
            elif result_code == 'INFO-200':
                end_loop = True
            else:
                print(f"error code: {result_code}")
                print(f'error msg: {root.find('.//MESSAGE').text}')
                end_loop = True
            start_index += page_size
            end_index += page_size
        else:
            print(f"api 호출 실패: {response.status_code}")
            exit()

    print("------------API 파싱 종료------------")

    df = transform_dataframe(df)

    file_name = f'{service}_all_{request_date}'  #  =========== custom value ===========
    file_path = f'{main_prefix}/request_date={request_date}/district=all/{file_name}'  # === custom value ===
    save_to_csv(df, bucket_name, f'{file_path}.csv')

    for district in districts:
        gu_df = df[df["GUNAME"].str.contains(district)]
        file_name = f'{service}_{district}_{request_date}'  #  =========== custom value ===========
        file_path = f'{main_prefix}/request_date={request_date}/district={district}/{file_name}'  #  =========== custom value ===========
        save_to_csv(gu_df, bucket_name, f'{file_path}.csv')


def parse_xml_data(root):
    '''
    xml 파싱 및 태그 추출
    '''
    data = []
    for rows in root.findall('.//row'):
        row = {}
        for child in rows:
            tag_name = child.tag
            tag_value = child.text.strip() if child.text else ''
            row[tag_name] = tag_value
        data.append(row)
    return pd.DataFrame(data)


def transform_dataframe(df):
    '''
    df 컬럼 변형
    '''
    ret_df = df
    ret_df['STRTDATE'] = pd.to_datetime(df['STRTDATE']).dt.date
    ret_df['END_DATE'] = pd.to_datetime(df['END_DATE']).dt.date
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
