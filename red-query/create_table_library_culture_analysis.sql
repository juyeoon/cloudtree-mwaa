CREATE TABLE library_culture_analysis (
    district VARCHAR(100),
    name VARCHAR(255),
    data_type VARCHAR(50),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    event_class VARCHAR(100),
    event_start_date DATE,
    event_end_date DATE,
    event_keywords VARCHAR(500),
    book_class_major VARCHAR(100),
    book_class_sub VARCHAR(100),
    loan_count BIGINT,
    cul_is_free BOOLEAN
);