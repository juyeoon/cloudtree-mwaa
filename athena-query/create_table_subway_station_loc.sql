CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_transformed_db.subway_station_loc (
    sta_id BIGINT,
    sta_name STRING,
    sta_latitude DOUBLE,
    sta_longitude DOUBLE
)
PARTITIONED BY (
    sta_district STRING,
    sta_line STRING
)
STORED AS PARQUET
LOCATION 's3://cloudtree-transformed-data/subway-station-loc/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
