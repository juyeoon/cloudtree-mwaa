CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_raw_db.cultural_event_parse_raw (
    CODENAME STRING,
    GUNAME STRING,
    TITLE STRING,
    DATE STRING,
    PLACE STRING,
    ORG_NAME STRING,
    USE_TRGT STRING,
    USE_FEE STRING,
    PLAYER STRING,
    PROGRAM STRING,
    ETC_DESC STRING,
    ORG_LINK STRING,
    MAIN_IMG STRING,
    RGSTDATE STRING,
    TICKET STRING,
    STRTDATE STRING,
    END_DATE STRING,
    THEMECODE STRING,
    LOT STRING,
    LAT STRING,
    IS_FREE STRING,
    HMPG_ADDR STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "skip.header.line.count" = "1"
)
LOCATION 's3://cloudtree-raw-data/cultural-event-parse/';
