CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_raw_db.library_data_raw (
	libCode STRING,
	libName STRING,
	address STRING,
	tel STRING,
	fax STRING,
	latitude double,
	longitude STRING,
	homepage STRING,
	closed STRING,
	operatingTime STRING,
	BookCount STRING
)
PARTITIONED BY (district STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
	"separatorChar" = ",",
	"skip.header.line.count" = "1"
)
LOCATION 's3://cloudtree-raw-data/library-data/';
