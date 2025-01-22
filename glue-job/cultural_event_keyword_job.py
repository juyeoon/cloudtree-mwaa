from datetime import datetime
import boto3
from bs4 import BeautifulSoup
from collections import Counter
import re
from kiwipiepy import Kiwi
from pyspark.sql import SparkSession
import pandas as pd
from io import StringIO
import requests

"""
job config:
+ Glue version: Glue 5.0 - Supports spark 3.5, Scala 2, Python 3
+ Type: Spark
+ Language: Python3
+ Worker type: G 1X
+ Job bookmark: Disable
+ Job timeout: 60 minutes
+ IAM Policies: AmazonS3FullAccess, AWSGlueConsoleFullAccess
"""


# SparkSession 초기화
spark = SparkSession.builder.appName("GlueJob").getOrCreate()

# S3 설정
s3 = boto3.client('s3')
bucket_name = 'cloudtree-raw-data'

# 형태소 분석기 초기화
kiwi = Kiwi()

# 제외 단어
excluded_words = [
    '금',
    '중',
    '일',
    '월',
    '서울',
    '문화',
    '정보',
    '신청',
    '행사',
    '관광',
    '축제',
    '문의',
    '홈페이지',
    '위치',
    '전체',
    '전화',
    '서울시',
    '년',
    '및',
    '기타',
]


def process_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        text_content = soup.get_text()

        korean_text = re.findall(r"[가-힣]+", text_content)
        korean_only = " ".join(korean_text)

        nouns = [word for word, tag, _, _ in kiwi.analyze(korean_only)[0][0] if tag.startswith('NN')]
        filtered_nouns = [noun for noun in nouns if noun not in excluded_words]
        noun_counts = Counter(filtered_nouns)
        return noun_counts.most_common(3)
    except Exception as e:
        return []


def save_to_csv(df, bucket, file_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    s3.put_object(Bucket=bucket, Key=file_key, Body=csv_buffer.getvalue(), ContentType='text/csv; charset=utf-8')


def get_latest_request_date_and_districts(bucket_name, prefix):
    s3 = boto3.client("s3")
    request_date_to_districts = {}
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if "Contents" not in response:
            break

        for obj in response["Contents"]:
            key = obj["Key"]
            if "request_date=" in key and "district=" in key:
                start_date = key.find("request_date=") + len("request_date=")
                end_date = key.find("/", start_date)
                request_date = key[start_date:end_date]

                start_district = key.find("district=") + len("district=")
                end_district = key.find("/", start_district) if "/" in key[start_district:] else len(key)
                district = key[start_district:end_district]

                if request_date not in request_date_to_districts:
                    request_date_to_districts[request_date] = []
                request_date_to_districts[request_date].append(district)

        if "NextContinuationToken" in response:
            continuation_token = response["NextContinuationToken"]
        else:
            break

    if not request_date_to_districts:
        print(f"No valid request_date and district found under prefix: {prefix}")
        return None, []

    latest_request_date = max(request_date_to_districts.keys())
    districts = request_date_to_districts[latest_request_date]
    return latest_request_date, districts


# 실행 예시
bucket_name = "cloudtree-raw-data"
prefix = "cultural-event-parse/"
request_date, districts = get_latest_request_date_and_districts(bucket_name, prefix)


for district in districts:
    file_key = f"cultural-event-parse/request_date={request_date}/district={district}/culturalEventInfo_{district}_{request_date}.csv"
    file_key2 = f"cultural-event-info/request_date={request_date}/district={district}/culturalEventInfo_{district}_{request_date}.csv"
    try:  # S3에서 CSV 파일 읽기
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        pandas_df = pd.read_csv(StringIO(content))
        # URL 처리 및 명사 추출
        pandas_df['Top_Nouns'] = pandas_df['HMPG_ADDR'].apply(lambda url: process_url(url) if pd.notnull(url) else [])
        pandas_df[['Keyword_1', 'Keyword_2', 'Keyword_3']] = (
            pandas_df['Top_Nouns']
            .apply(lambda x: [x[i][0] if i < len(x) else None for i in range(3)])
            .apply(pd.Series)
        )
        pandas_df.drop(columns=['Top_Nouns'], inplace=True)  # S3에 저장
        save_to_csv(pandas_df, bucket_name, file_key2)
        print(f"File successfully uploaded to {file_key2}")
    except Exception as e:
        print(f"Error: {e}")
