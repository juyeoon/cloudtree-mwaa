CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_transformed_db.cultural_event_info (
	cul_class STRING,
    cul_title STRING,
    cul_place STRING,
    cul_org_name STRING,
    cul_trgt_age STRING,
    cul_fee	STRING,
    cul_org_url STRING,
    cul_main_img STRING,
    cul_rgst_date DATE,
    cul_organizer_type STRING,
    cul_start_date DATE,
    cul_end_date DATE,
    cul_latitude DOUBLE,
    cul_longitude DOUBLE,
    cul_is_free	boolean,
    cul_hmpg_url STRING,
    cul_keyword_1 STRING,
    cul_keyword_2 STRING,
    cul_keyword_3 STRING
)
PARTITIONED BY (
    cul_district STRING
)
STORED AS PARQUET
LOCATION 's3://cloudtree-transformed-data/cultural-event-info/';
