CREATE EXTERNAL TABLE IF NOT EXISTS cloudtree_transformed_db.best_loan_list (
    bll_book_name STRING,
    bll_authors STRING,
    bll_publisher STRING,
    bll_publication_year INT,
    bll_isbn13 BIGINT,
    bll_addition_symbol INT,
    bll_vol INT,
    bll_class_no DOUBLE,
    bll_class_major STRING,
    bll_class_sub STRING,
    bll_class_detailed STRING,
    bll_loan_count BIGINT
)
PARTITIONED BY (
    bll_district STRING,
    bll_year INT,
    bll_month INT
)
STORED AS PARQUET
LOCATION 's3://cloudtree-transformed-data/best-loan-list/';
