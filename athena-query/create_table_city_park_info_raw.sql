CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_raw_db.city_park_info_raw (
    management_number STRING,
    park_name STRING,
    park_type STRING,
    road_address STRING,
    lot_address STRING,
    latitude STRING,
    longitude STRING,
    area STRING,
    exercise_facilities STRING,
    recreation_facilities STRING,
    convenience_facilities STRING,
    education_facilities STRING,
    other_facilities STRING,
    designation_date STRING,
    management_agency STRING,
    phone STRING,
    data_date STRING,
    provider_code STRING,
    provider_name STRING
)
PARTITIONED BY (
    request_date STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "skip.header.line.count" = "1",
    "serialization.encoding" = "EUC-KR"
)
LOCATION 's3://cloudtree-raw-data/city-park-info/';
