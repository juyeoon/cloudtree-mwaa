import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType
from awsglue.dynamicframe import DynamicFrame
import os

"""
job config:
+ Glue version: Glue 5.0 - Supports spark 3.5, Scala 2, Python 3
+ Type: Spark
+ Language: Python3
+ Worker type: G 1X
+ Job bookmark: Enable
+ Job timeout: 15 minutes
+ IAM Policies: AdministratorAccess, AmazonAthenaFullAccess, AmazonRedshiftAllCommandsFullAccess, AmazonRedshiftFullAccess, AmazonS3FullAccess, AWSGlueConsoleFullAccess (최소 권한 아님)
"""

# Glue Context 및 SparkContext 초기화
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Glue Job 초기화
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 경로 및 출력 경로 설정
input_path = "s3://cloudtree-raw-data/best-loan-list/"
output_path = "s3://cloudtree-transformed-data/best-loan-list/"


# 북마크를 S3 경로로 관리하는 함수
def get_last_processed_s3_path():
    try:
        bookmark_state = job.bookmark()
        print("Job Bookmark State: ", bookmark_state)
        return bookmark_state
    except Exception as e:
        print("Error retrieving bookmark state: ", str(e))
        return None


def update_bookmark_state(last_processed_path):
    try:
        job.bookmark(last_processed_path)
        print(f"Updated job bookmark state to: {last_processed_path}")
    except Exception as e:
        print("Error updating bookmark state: ", str(e))


# 데이터 로딩 (Job Bookmark 상태에 따라 로드)
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "separator": ","},
    transformation_ctx="data_loading",
)

# 데이터가 비어 있으면 Job 종료하지 않고 메시지 출력
if dynamic_frame.count() == 0:
    print("No new data to process. Skipping ETL process.")
else:
    # 데이터프레임 변환 및 처리
    data_frame = dynamic_frame.toDF()

    # 컬럼 이름 변경 및 데이터 타입 지정
    data_frame = (
        data_frame.withColumnRenamed("bookname", "bll_book_name")
        .withColumnRenamed("authors", "bll_authors")
        .withColumnRenamed("publisher", "bll_publisher")
        .withColumnRenamed("publication_year", "bll_publication_year")
        .withColumnRenamed("isbn13", "bll_isbn13")
        .withColumnRenamed("addition_symbol", "bll_addition_symbol")
        .withColumnRenamed("vol", "bll_vol")
        .withColumnRenamed("class_no", "bll_class_no")
        .withColumnRenamed("class_nm", "bll_class_major")
        .withColumnRenamed("class_major", "bll_class_major")
        .withColumnRenamed("class_sub", "bll_class_sub")
        .withColumnRenamed("class_detailed", "bll_class_detailed")
        .withColumnRenamed("loan_count", "bll_loan_count")
        .withColumnRenamed("district", "bll_district")
        .withColumnRenamed("year", "bll_year")
        .withColumnRenamed("month", "bll_month")
    )

    # 각 컬럼의 데이터 타입을 명시적으로 지정
    data_frame = (
        data_frame.withColumn("bll_book_name", data_frame["bll_book_name"].cast(StringType()))
        .withColumn("bll_authors", data_frame["bll_authors"].cast(StringType()))
        .withColumn("bll_publisher", data_frame["bll_publisher"].cast(StringType()))
        .withColumn("bll_publication_year", data_frame["bll_publication_year"].cast(IntegerType()))
        .withColumn("bll_isbn13", data_frame["bll_isbn13"].cast(LongType()))
        .withColumn("bll_addition_symbol", data_frame["bll_addition_symbol"].cast(IntegerType()))
        .withColumn("bll_vol", data_frame["bll_vol"].cast(IntegerType()))
        .withColumn("bll_class_no", data_frame["bll_class_no"].cast(DoubleType()))
        .withColumn("bll_class_major", data_frame["bll_class_major"].cast(StringType()))
        .withColumn("bll_class_sub", data_frame["bll_class_sub"].cast(StringType()))
        .withColumn("bll_class_detailed", data_frame["bll_class_detailed"].cast(StringType()))
        .withColumn("bll_loan_count", data_frame["bll_loan_count"].cast(LongType()))
        .withColumn("bll_district", data_frame["bll_district"].cast(StringType()))
        .withColumn("bll_year", data_frame["bll_year"].cast(IntegerType()))
        .withColumn("bll_month", data_frame["bll_month"].cast(IntegerType()))
    )

    # DataFrame을 DynamicFrame으로 변환
    dynamic_frame_transformed = DynamicFrame.fromDF(data_frame, glueContext, "dynamic_frame_transformed")

    # 파케이 파일 형식으로 저장
    glueContext.write_dynamic_frame.from_options(
        dynamic_frame_transformed,
        connection_type="s3",
        connection_options={"path": output_path, "partitionKeys": ["bll_district", "bll_year", "bll_month"]},
        format="parquet",
        transformation_ctx="data_saving",
    )

    # 마지막 처리된 경로로 북마크 업데이트
    last_processed_path = f"{input_path}district={data_frame.select('bll_district').first()['bll_district']}/year={data_frame.select('bll_year').first()['bll_year']}/month={data_frame.select('bll_month').first()['bll_month']}/"
    update_bookmark_state(last_processed_path)

    print("ETL 작업이 성공적으로 완료되었습니다.")

# Job Commit
job.commit()
