import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from datetime import datetime
import boto3

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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""


def delete_existing_data(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()


request_date = datetime.now().date().strftime('%y%m%d')


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
prefix = "library-data/"
request_date = get_latest_request_date(input_bucket_name, prefix)


# 기존 데이터를 삭제
bucket_name = "cloudtree-transformed-data"
output_prefix = "library-data/"
delete_existing_data(bucket_name, output_prefix)


# Script generated for node Amazon S3
AmazonS3_node1736398211126 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"s3://cloudtree-raw-data/library-data/request_date={request_date}"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1736398211126",
)

# Script generated for node Change Schema
ChangeSchema_node1736398260997 = ApplyMapping.apply(
    frame=AmazonS3_node1736398211126,
    mappings=[
        ("libname", "string", "lib_name", "string"),
        ("latitude", "string", "lib_latitude", "double"),
        ("longitude", "string", "lib_longitude", "double"),
        ("homepage", "string", "lib_homepage", "string"),
        ("bookcount", "string", "lib_book_count", "bigint"),
        ("district", "string", "lib_district", "string"),
    ],
    transformation_ctx="ChangeSchema_node1736398260997",
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(
    frame=ChangeSchema_node1736398260997,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1736395546556",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"},
)
AmazonS3_node1736398442461 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1736398260997,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://cloudtree-transformed-data/library-data/", "partitionKeys": ["lib_district"]},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1736398442461",
)

job.commit()
