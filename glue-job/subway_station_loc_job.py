import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from datetime import datetime
from botocore.config import Config

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
    # 재시도 설정
    config = Config(retries={'max_attempts': 2, 'mode': 'adaptive'})  # 최대 재시도 횟수  # 속도 제한에 맞춰 재시도
    s3 = boto3.resource('s3', config=config)
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        try:
            obj.delete()
            print(f"Deleted: {obj.key}")
        except Exception as e:
            print(f"Error deleting {obj.key}: {e}")


# Glue Job 인자 가져오기
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# 이미 존재하는 SparkContext를 재사용
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# 가장 나중 날짜를 확인
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
prefix = "subway-station-loc/"
request_date = get_latest_request_date(input_bucket_name, prefix)

# 지하철 마스터 데이터 로드
subway_master_df = spark.read.csv(
    f"s3://cloudtree-raw-data/subway-station-loc/request_date={request_date}/",
    header=True,
    inferSchema=True,
    encoding="UTF-8",
)

# 구 정보 데이터 로드
district_info_df = spark.read.csv(
    "s3://cloudtree-config/Seoul_Subway_District_Info.CSV", header=True, inferSchema=True, encoding="CP949"
).toDF("자치구", "해당역(호선)", "역개수")

# 기존 데이터를 삭제
bucket_name = "cloudtree-transformed-data"
output_prefix = "subway-station-loc/"
delete_existing_data(bucket_name, output_prefix)

# 구 정보 데이터 처리
district_info_exploded = district_info_df.select(
    F.col("자치구").alias("sta_district"), F.split(F.col("해당역(호선)"), ", ").alias("stations")
).withColumn("station", F.explode(F.col("stations")))

district_info_cleaned = district_info_exploded.withColumn(
    "station_name", F.trim(F.regexp_replace(F.col("station"), r"\(.*?\)", ""))
).select("sta_district", "station_name")

# 지하철 마스터 데이터에서 ROUTE 컬럼 정제
subway_master_df = subway_master_df.withColumn("ROUTE", F.trim(F.regexp_replace(F.col("ROUTE"), r"\(.*?\)", "")))

# 데이터 조인
result_df = subway_master_df.join(
    district_info_cleaned, subway_master_df["BLDN_NM"] == district_info_cleaned["station_name"], "left"
).drop("station_name")

# 최종 데이터 선택 및 타입 변환
final_df = result_df.select(
    F.col("BLDN_ID").alias("sta_id").cast("bigint"),
    F.col("BLDN_NM").alias("sta_name").cast("string"),
    F.col("LAT").alias("sta_latitude").cast("double"),
    F.col("LOT").alias("sta_longitude").cast("double"),
    F.col("ROUTE").alias("sta_line").cast("string"),
    F.col("sta_district").cast("string"),
)

# 중복 제거 (sta_name과 sta_line 기준)
deduplicated_df = final_df.dropDuplicates(["sta_name", "sta_line"])

# 중복 제거 후 데이터를 S3에 저장
deduplicated_df.write.partitionBy("sta_district", "sta_line").parquet(
    "s3://cloudtree-transformed-data/subway-station-loc/", mode="overwrite", compression="snappy"
)

# Glue Job 완료
print("ETL processing complete with duplicates removed.")
job.commit()
