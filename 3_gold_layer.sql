-- FILE: 3_gold_layer.sql
--------------------------------------------------------------------------------
-- GOLD LAYER: Business-Ready Analytics
-- Purpose: To generate highly aggregated and modeled data for BI tools and 
-- business users, using only the fully validated data (is_valid_record = TRUE).
--------------------------------------------------------------------------------

-- 1. Create Schema
CREATE SCHEMA IF NOT EXISTS gold;

-- 2. Drop existing daily summary table
DROP TABLE IF EXISTS gold.daily_trip_summary;

-- 3. Create Daily Trip Summary Table (Core Analytic Model)
CREATE TABLE gold.daily_trip_summary AS
SELECT
    DATE(pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    COUNT(DISTINCT vendor_id) AS active_vendors,
    
    -- Financial Metrics
    SUM(total_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare_amount,
    SUM(tip_amount) AS total_tips,
    
    -- Trip Metrics
    AVG(trip_distance_miles) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
    AVG(trip_speed_mph) AS avg_trip_speed_mph
FROM
    silver.taxi_trips_cleaned
WHERE 
    is_valid_record = TRUE -- CRITICAL: Only use validated data
GROUP BY
    1
ORDER BY
    1;

-- 4. Create Vendor Performance Analysis Table
DROP TABLE IF EXISTS gold.vendor_performance;
CREATE TABLE gold.vendor_performance AS
SELECT
    -- Assign readable names to IDs
    CASE 
        WHEN vendor_id = '1' THEN 'Creative Mobile Technologies'
        WHEN vendor_id = '2' THEN 'Verifone Inc.'
        ELSE 'Other'
    END AS vendor_name,
    COUNT(*) AS total_trips,
    AVG(tip_amount) AS avg_tip_per_trip,
    SUM(total_amount) AS total_revenue,
    (SUM(tip_amount) * 100.0 / SUM(total_amount)) AS tip_percentage_of_revenue
FROM
    silver.taxi_trips_cleaned
WHERE 
    is_valid_record = TRUE
GROUP BY
    1, 
    vendor_id
ORDER BY
    total_trips DESC;

-- 5. Create Executive Summary VIEW (Pre-calculated dashboard data)
-- This view provides an easy way to compare metrics across different periods.
CREATE OR REPLACE VIEW gold.executive_summary_dashboard AS
SELECT
    (SELECT total_trips FROM gold.daily_trip_summary WHERE trip_date = CURRENT_DATE) AS today_trips,
    (SELECT total_revenue FROM gold.daily_trip_summary WHERE trip_date = CURRENT_DATE) AS today_revenue,
    (SELECT total_trips FROM gold.daily_trip_summary WHERE trip_date = CURRENT_DATE - INTERVAL '1 day') AS yesterday_trips,
    (SELECT total_revenue FROM gold.daily_trip_summary WHERE trip_date = CURRENT_DATE - INTERVAL '1 day') AS yesterday_revenue;

-- 6. Verify Gold Output
SELECT * FROM gold.daily_trip_summary LIMIT 5;
SELECT * FROM gold.vendor_performance;
SELECT * FROM gold.executive_summary_dashboard;
