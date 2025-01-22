CREATE TABLE facility_locations (
    facility_type VARCHAR(50),
    facility_name VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    district VARCHAR(100)
);

INSERT INTO facility_locations
SELECT 
    'Bus' AS facility_type,
    bus_name AS facility_name,
    bus_latitude AS latitude,
    bus_longitude AS longitude,
    bus_district AS district
FROM spectrum_schema.bus_stop_loc
UNION ALL
SELECT 
    'Subway' AS facility_type,
    sta_name AS facility_name,
    sta_latitude AS latitude,
    sta_longitude AS longitude,
    sta_district AS district
FROM spectrum_schema.subway_station_loc
UNION ALL
SELECT 
    'Park' AS facility_type,
    park_name AS facility_name,
    park_latitude AS latitude,
    park_longitude AS longitude,
    park_district AS district
FROM spectrum_schema.city_park_info;