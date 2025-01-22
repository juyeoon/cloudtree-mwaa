import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when
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


# S3 경로 정리 함수
def delete_existing_data(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()


# 기존 데이터를 삭제
bucket_name = "cloudtree-transformed-data"
output_prefix = "cultural-event-info/"
delete_existing_data(bucket_name, output_prefix)


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
prefix = "cultural-event-info/"
request_date = get_latest_request_date(input_bucket_name, prefix)

# Load data from S3
input_path = f"s3://cloudtree-raw-data/cultural-event-info/request_date={request_date}/"
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("quote", "\"")
    .option("sep", ",")
    .option("encoding", "UTF-8")
    .load(input_path)
)

# Apply transformations
# Rename columns and cast types
df = df.select(
    col("CODENAME").alias("cul_class"),
    col("GUNAME").alias("cul_district"),
    col("TITLE").alias("cul_title"),
    col("PLACE").alias("cul_place"),
    col("ORG_NAME").alias("cul_org_name"),
    col("USE_TRGT").alias("cul_trgt_age"),
    col("USE_FEE").alias("cul_fee"),
    col("ORG_LINK").alias("cul_org_url"),
    col("MAIN_IMG").alias("cul_main_img"),
    col("RGSTDATE").alias("cul_rgst_date").cast("date"),
    col("TICKET").alias("cul_organizer_type"),
    col("STRTDATE").alias("cul_start_date").cast("date"),
    col("END_DATE").alias("cul_end_date").cast("date"),
    col("LOT").alias("cul_latitude").cast("double"),
    col("LAT").alias("cul_longitude").cast("double"),
    col("IS_FREE").alias("cul_is_free"),
    col("HMPG_ADDR").alias("cul_hmpg_url"),
    col("Keyword_1").alias("cul_keyword_1"),
    col("Keyword_2").alias("cul_keyword_2"),
    col("Keyword_3").alias("cul_keyword_3"),
)

# Transform "cul_is_free" column
df = df.withColumn(
    "cul_is_free", when(col("cul_is_free") == "무료", True).when(col("cul_is_free") == "유료", False).otherwise(None)
)

# Write transformed data to S3 in Parquet format with partitioning
output_path = "s3://cloudtree-transformed-data/cultural-event-info/"
df.write.format("parquet").mode("overwrite").partitionBy("cul_district").save(output_path)

print("ETL Job completed successfully!")
