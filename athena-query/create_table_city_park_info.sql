CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_transformed_db.city_park_info (
    park_name STRING,
    park_type STRING,
    lot_address STRING,
    park_latitude DOUBLE,
    park_longitude DOUBLE,
    park_area STRING,
    park_designation_date date,
    park_mgmt_agency STRING,
    park_phone STRING,
    park_data_date DATE,
    park_provider_code bigint,
    park_provider_name string
)
PARTITIONED BY (
    park_district STRING
)
STORED AS PARQUET
LOCATION 's3://cloudtree-transformed-data/city-park-info/';
