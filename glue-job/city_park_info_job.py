import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
import boto3
from datetime import datetime

"""
job config:
+ Glue version: Glue 5.0 - Supports spark 3.5, Scala 2, Python 3
+ Type: Spark
+ Language: Python3
+ Worker type: G 1X
+ Job bookmark: Disable
+ Job timeout: 15 minutes
+ IAM Policies: AdministratorAccess, AmazonAthenaFullAccess, AmazonRedshiftAllCommandsFullAccess, AmazonRedshiftFullAccess, AmazonS3FullAccess, AWSGlueConsoleFullAccess (최소 권한 아님)
"""

# Parse arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark Context and Session
sc = SparkContext()
spark = SparkSession(sc)


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
prefix = "city-park-info/"
request_date = get_latest_request_date(input_bucket_name, prefix)


# Load data from S3
input_path = f"s3://cloudtree-raw-data/city-park-info/request_date={request_date}/"
df = spark.read.format("csv").option("header", "true").option("quote", "\"").option("sep", ",").load(input_path)

# Apply transformations
# Rename columns and cast types
df = df.select(
    col("공원명").alias("park_name"),
    col("공원구분").alias("park_type"),
    col("소재지지번주소").alias("lot_address"),
    col("위도").alias("park_latitude").cast("double"),
    col("경도").alias("park_longitude").cast("double"),
    col("공원면적").alias("park_area"),
    col("지정고시일").alias("park_designation_date").cast("date"),
    col("관리기관명").alias("park_mgmt_agency"),
    col("전화번호").alias("park_phone"),
    col("데이터기준일자").alias("park_data_date").cast("date"),
    col("제공기관코드").alias("park_provider_code").cast("bigint"),
    col("제공기관명").alias("park_provider_name"),
)


# Extract district name from lot_address and add a new column
# pattern = r"\s([가-힣]+구)"
# df = df.withColumn("park_district", regexp_extract(col("lot_address"), pattern, 1))

pattern = r"서울특별시\s([가-힣]+구)"

# park_district 컬럼 추가
df = df.withColumn("park_district", regexp_extract(col("park_provider_name"), pattern, 1))


def delete_existing_data(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()


bucket_name = "cloudtree-transformed-data"
output_prefix = "city-park-info/"

# 기존 데이터를 삭제
delete_existing_data(bucket_name, output_prefix)

# Write transformed data to S3 in Parquet format with partitioning
output_path = "s3://cloudtree-transformed-data/city-park-info/"
df.write.format("parquet").mode("overwrite").partitionBy("park_district").save(output_path)

print("ETL Job completed successfully!")
