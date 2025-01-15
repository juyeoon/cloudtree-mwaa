CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_transformed_db.bus_stop_loc (
    bus_name STRING,
    bus_latitude STRING,
    bus_longitude STRING
)
PARTITIONED BY (
    bus_district STRING
)
STORED AS PARQUET
LOCATION 's3://cloudtree-transformed-data/bus-stop-loc/';
