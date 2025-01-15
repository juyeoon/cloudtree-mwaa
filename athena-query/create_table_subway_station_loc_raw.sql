CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_raw_db.subway_station_loc_raw (
    BLDN_ID STRING,
    BLDN_NM STRING,
    ROUTE STRING,
    LAT STRING,
    LOT STRING
)
PARTITIONED BY (
    request_date STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "skip.header.line.count" = "1"
)
LOCATION 's3://cloudtree-raw-data/subway-station-loc/';
