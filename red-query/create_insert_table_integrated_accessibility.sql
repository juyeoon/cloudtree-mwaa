CREATE TABLE integrated_accessibility (
    entity_name VARCHAR(255),
    entity_district VARCHAR(100),
    data_type VARCHAR(50),
    entity_latitude DOUBLE PRECISION,
    entity_longitude DOUBLE PRECISION,
    event_class VARCHAR(255),
    event_start_date DATE,
    event_end_date DATE,
    event_keywords VARCHAR(500),
    book_class_major VARCHAR(255),
    book_class_sub VARCHAR(255),
    loan_count BIGINT,
    cul_is_free BOOLEAN,
    bus_stops_within_1km INT,
    subway_stations_within_1km INT,
    parks_within_1km INT
);

INSERT INTO integrated_accessibility
SELECT 
    u.name AS entity_name,
    u.district AS entity_district,
    u.data_type,
    u.latitude AS entity_latitude,
    u.longitude AS entity_longitude,
    u.event_class,
    u.event_start_date,
    u.event_end_date,
    u.event_keywords,
    u.book_class_major,
    u.book_class_sub,
    u.loan_count,
    u.cul_is_free,
    (SELECT COUNT(DISTINCT bus_name) FROM spectrum_schema.bus_stop_loc 
     WHERE bus_district = u.district 
     AND ST_DistanceSphere(ST_Point(u.longitude, u.latitude), 
                           ST_Point(bus_longitude, bus_latitude)) <= 1000) AS bus_stops_within_1km,
    (SELECT COUNT(DISTINCT sta_name) FROM spectrum_schema.subway_station_loc 
     WHERE sta_district = u.district 
     AND ST_DistanceSphere(ST_Point(u.longitude, u.latitude), 
                           ST_Point(sta_longitude, sta_latitude)) <= 1000) AS subway_stations_within_1km,
    (SELECT COUNT(DISTINCT park_name) FROM spectrum_schema.city_park_info 
     WHERE park_district = u.district 
     AND ST_DistanceSphere(ST_Point(u.longitude, u.latitude), 
                           ST_Point(park_longitude, park_latitude)) <= 1000) AS parks_within_1km
FROM library_culture_analysis u
WHERE u.data_type IN ('Library', 'Event', 'Book');