CREATE TABLE analysis_results (
    entity_name VARCHAR(255),
    entity_district VARCHAR(100),
    metric_name VARCHAR(100),
    metric_value BIGINT
);

INSERT INTO analysis_results (entity_name, entity_district, metric_name, metric_value)
SELECT 
    u.name AS entity_name,
    u.district AS entity_district,
    'Nearby Bus Stops' AS metric_name,
    COUNT(DISTINCT b.bus_name) AS metric_value
FROM 
    public.library_culuture_analysis u
JOIN 
    spectrum_schema.bus_stop_loc b
ON 
    u.district = b.bus_district
AND 
    ST_DistanceSphere(
        ST_Point(u.longitude, u.latitude),
        ST_Point(b.bus_longitude, b.bus_latitude)
    ) <= 500
WHERE 
    u.data_type = 'Library'
GROUP BY 
    u.name, u.district;
    
INSERT INTO analysis_results (entity_name, entity_district, metric_name, metric_value)
SELECT 
    u.name AS entity_name,
    u.district AS entity_district,
    'Nearby Parks' AS metric_name,
    COUNT(DISTINCT p.park_name) AS metric_value
FROM 
    public.library_culuture_analysis u
JOIN 
    spectrum_schema.city_park_info p
ON 
    u.district = p.park_district
AND 
    ST_DistanceSphere(
        ST_Point(u.longitude, u.latitude),
        ST_Point(p.park_longitude, p.park_latitude)
    ) <= 1000
WHERE 
    u.data_type = 'Event'
GROUP BY 
    u.name, u.district;