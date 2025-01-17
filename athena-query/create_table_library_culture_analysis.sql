CREATE TABLE cloudtree_aggregated_db.library_culture_analysis
WITH (
    format = 'PARQUET', 
    external_location = 's3://cloudtree-aggregated-data/library_culture_analysis/', 
    write_compression = 'SNAPPY' 
) AS
SELECT 
    lib.lib_district AS district,
    lib.lib_name AS name,
    'Library' AS data_type,
    lib.lib_latitude AS latitude,
    lib.lib_longitude AS longitude,
    NULL AS event_class,
    NULL AS event_start_date,
    NULL AS event_end_date,
    NULL AS event_keywords,
    NULL AS book_class_major,
    NULL AS book_class_sub,
    NULL AS loan_count,
    NULL AS cul_is_free
FROM 
    cloudtree_transformed_db.library_data lib

UNION ALL

SELECT 
    cul.cul_district AS district,
    cul.cul_title AS name,
    'Event' AS data_type,
    cul.cul_latitude AS latitude,
    cul.cul_longitude AS longitude,
    cul.cul_class AS event_class,
    cul.cul_start_date AS event_start_date,
    cul.cul_end_date AS event_end_date,
    CONCAT_WS(',', cul.cul_keyword_1, cul.cul_keyword_2, cul.cul_keyword_3) AS event_keywords,
    NULL AS book_class_major,
    NULL AS book_class_sub,
    NULL AS loan_count,
    cul.cul_is_free AS cul_is_free
FROM 
    cloudtree_transformed_db.cultural_event_info cul

UNION ALL

SELECT 
    bll.bll_district AS district,
    bll.bll_book_name AS name,
    'Book' AS data_type,
    NULL AS latitude,
    NULL AS longitude,
    NULL AS event_class,
    NULL AS event_start_date,
    NULL AS event_end_date,
    NULL AS event_keywords,
    bll.bll_class_major AS book_class_major,
    bll.bll_class_sub AS book_class_sub,
    bll.bll_loan_count AS loan_count,
    NULL AS cul_is_free
FROM 
    cloudtree_transformed_db.best_loan_list bll;