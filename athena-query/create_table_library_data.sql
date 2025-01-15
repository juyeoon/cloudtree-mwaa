CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_transformed_db.library_data (
    lib_name string,
    lib_latitude double,
    lib_longitude double,
    lib_homepage string,
    lib_book_count bigint
)
PARTITIONED BY (
    lib_district string
)
STORED AS PARQUET
LOCATION 's3://cloudtree-transformed-data/library-data/';
