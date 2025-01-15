CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_raw_db.best_loan_list_raw (
    no STRING,
    ranking STRING,
    bookname STRING,
    authors STRING,
    publisher STRING,
    publication_year STRING,
    isbn13 STRING,
    addition_symbol STRING,
    vol STRING,
    class_no STRING,
    class_nm STRING,
    loan_count STRING,
    bookImageURL STRING,
    bookDtlUrl STRING,
    class_major STRING,
    class_sub STRING,
    class_detailed STRING
)
PARTITIONED BY (district STRING, year STRING, month STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "skip.header.line.count" = "1"
)
LOCATION 's3://cloudtree-raw-data/best-loan-list/';
