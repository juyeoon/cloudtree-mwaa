import boto3
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

"""
job config:
+ Glue version: Glue 5.0 - Supports spark 3.5, Scala 2, Python 3
+ Type: Spark
+ Language: Python3
+ Worker type: G 1X
+ Job bookmark: Disable
+ Job timeout: 15 minutes
+ IAM Policies: AmazonS3FullAccess, AWSGlueConsoleFullAccess
"""


# S3 경로 정리 함수
def delete_existing_data(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()


# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize GlueContext and SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def get_latest_request_date(bucket_name, prefix):
    # S3 클라이언트 생성
    s3 = boto3.client("s3")

    # S3 객체 목록 가져오기
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" not in response:
        raise ValueError("No objects found under the given prefix.")

    # request_date 값 추출
    request_dates = []
    for obj in response["Contents"]:
        key = obj["Key"]
        if "request_date=" in key:
            # request_date 값만 추출
            start = key.find("request_date=") + len("request_date=")
            end = key.find("/", start)  # "/"로 끝나는 부분까지 추출
            if end != -1:
                request_date = key[start:end]
                request_dates.append(request_date)

    if not request_dates:
        raise ValueError("No request_date found in the keys.")

    # 가장 큰 request_date 찾기
    latest_request_date = max(request_dates)

    return latest_request_date


input_bucket_name = "cloudtree-raw-data"
prefix = "bus-stop-loc/"
request_date = get_latest_request_date(input_bucket_name, prefix)
# Input and output paths
bus_input_path = f"s3://cloudtree-raw-data/bus-stop-loc/request_date={request_date}/"
bus_output_path = "s3://cloudtree-transformed-data/bus-stop-loc/"
bucket_name = "cloudtree-transformed-data"
output_prefix = "bus-stop-loc/"

# 기존 데이터를 삭제
delete_existing_data(bucket_name, output_prefix)

# Transform bus stop location data
bus_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [bus_input_path], "recurse": True},
)

bus_df = bus_dynamic_frame.toDF()

# Add district based on CRTR_ID
bus_df = bus_df.withColumn(
    "bus_district",
    when(col("CRTR_ID").substr(1, 3) == "100", "종로구")
    .when(col("CRTR_ID").substr(1, 3) == "101", "중구")
    .when(col("CRTR_ID").substr(1, 3) == "102", "용산구")
    .when(col("CRTR_ID").substr(1, 3) == "103", "성동구")
    .when(col("CRTR_ID").substr(1, 3) == "104", "광진구")
    .when(col("CRTR_ID").substr(1, 3) == "105", "동대문구")
    .when(col("CRTR_ID").substr(1, 3) == "106", "중랑구")
    .when(col("CRTR_ID").substr(1, 3) == "107", "성북구")
    .when(col("CRTR_ID").substr(1, 3) == "108", "강북구")
    .when(col("CRTR_ID").substr(1, 3) == "109", "도봉구")
    .when(col("CRTR_ID").substr(1, 3) == "110", "노원구")
    .when(col("CRTR_ID").substr(1, 3) == "111", "은평구")
    .when(col("CRTR_ID").substr(1, 3) == "112", "서대문구")
    .when(col("CRTR_ID").substr(1, 3) == "113", "마포구")
    .when(col("CRTR_ID").substr(1, 3) == "114", "양천구")
    .when(col("CRTR_ID").substr(1, 3) == "115", "강서구")
    .when(col("CRTR_ID").substr(1, 3) == "116", "구로구")
    .when(col("CRTR_ID").substr(1, 3) == "117", "금천구")
    .when(col("CRTR_ID").substr(1, 3) == "118", "영등포구")
    .when(col("CRTR_ID").substr(1, 3) == "119", "동작구")
    .when(col("CRTR_ID").substr(1, 3) == "120", "관악구")
    .when(col("CRTR_ID").substr(1, 3) == "121", "서초구")
    .when(col("CRTR_ID").substr(1, 3) == "122", "강남구")
    .when(col("CRTR_ID").substr(1, 3) == "123", "송파구")
    .when(col("CRTR_ID").substr(1, 3) == "124", "강동구")
    .otherwise(None),
)
bus_df = bus_df.filter(col("bus_district").isNotNull())

# Select columns with aliases and cast
bus_df = bus_df.select(
    col("CRTR_NM").alias("bus_name"),
    col("LAT").cast("double").alias("bus_latitude"),
    col("LOT").cast("double").alias("bus_longitude"),
    col("bus_district").alias("bus_district"),
)

bus_dynamic_frame = DynamicFrame.fromDF(bus_df, glueContext, "bus_dynamic_frame")

# Write bus data
bus_output = glueContext.write_dynamic_frame.from_options(
    frame=bus_dynamic_frame,
    connection_type="s3",
    format="parquet",
    connection_options={"path": bus_output_path, "partitionKeys": ["bus_district"], "mode": "overwrite"},
    format_options={"compression": "snappy"},
)

# Commit job
job.commit()
