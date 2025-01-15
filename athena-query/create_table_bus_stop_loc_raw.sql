CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_raw_db.bus_stop_loc_raw (
	CRTR_ID STRING,
	CRTR_NM STRING,
	CRTR_TYPE STRING,
	CRTR_NO STRING,
    LAT STRING,
	LOT STRING,
	BUS_ARVL_INFO_GUIDEM_INSTL STRING
)
PARTITIONED BY (
    request_date STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
	"separatorChar" = ",",
	"skip.header.line.count" = "1"
)
LOCATION 's3://cloudtree-raw-data/bus-stop-loc/';
