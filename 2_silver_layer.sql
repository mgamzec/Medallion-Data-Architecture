-- FILE: 2_silver_layer.sql
--------------------------------------------------------------------------------
-- SILVER LAYER: Cleaned and Conformed Data
-- Purpose: To apply data quality checks, standardization, and basic 
-- feature engineering before analytical use. Bad data is flagged, not deleted.
--------------------------------------------------------------------------------

-- 1. Create Schema
CREATE SCHEMA IF NOT EXISTS silver;

-- 2. Drop existing cleaned table
DROP TABLE IF EXISTS silver.taxi_trips_cleaned;

-- 3. Create Cleaned Table Structure
CREATE TABLE silver.taxi_trips_cleaned (
    trip_surrogate_key SERIAL PRIMARY KEY, 
    
    -- Metadata
    bronze_load_timestamp TIMESTAMP WITHOUT TIME ZONE,
    silver_load_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    
    -- Cleaned Trip Info
    vendor_id TEXT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count_cleaned INTEGER, 
    trip_distance_miles NUMERIC,
    payment_type_name TEXT, 
    fare_amount NUMERIC,
    tip_amount NUMERIC,
    total_amount NUMERIC,
    pickup_longitude NUMERIC,
    pickup_latitude NUMERIC,
    dropoff_longitude NUMERIC,
    dropoff_latitude NUMERIC,
    
    -- Derived Fields
    trip_duration_minutes NUMERIC, 
    trip_speed_mph NUMERIC, 

    -- Data Quality Flags (Key for quarantining bad data)
    is_valid_coordinates BOOLEAN, 
    is_valid_fare BOOLEAN, 
    is_valid_duration BOOLEAN, 
    is_valid_record BOOLEAN -- True if all basic checks pass
);

-- 4. Create Index on is_valid_record for fast filtering of bad data
CREATE INDEX IF NOT EXISTS idx_silver_valid_trips ON silver.taxi_trips_cleaned (is_valid_record); 

-- 5. ELT Step: Insert, Clean, and Transform Data from Bronze
INSERT INTO silver.taxi_trips_cleaned (
    bronze_load_timestamp, silver_load_timestamp, source_file, vendor_id, pickup_datetime, dropoff_datetime, 
    passenger_count_cleaned, trip_distance_miles, payment_type_name, fare_amount, tip_amount, total_amount, 
    pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, trip_duration_minutes, trip_speed_mph, 
    is_valid_coordinates, is_valid_fare, is_valid_duration, is_valid_record
)
SELECT
    T1.load_timestamp, NOW(), T1.source_file, T1.vendor_id, T1.pickup_datetime, T1.dropoff_datetime, 
    -- Rule 1: Passenger count constraint (1-6)
    CASE WHEN T1.passenger_count < 1 THEN 1 WHEN T1.passenger_count > 6 THEN 6 ELSE T1.passenger_count END AS passenger_count_cleaned,
    T1.trip_distance AS trip_distance_miles,
    -- Rule 2: Payment type standardization
    CASE WHEN T1.payment_type = 1 THEN 'Credit Card' WHEN T1.payment_type = 2 THEN 'Cash' WHEN T1.payment_type = 3 THEN 'No Charge' WHEN T1.payment_type = 4 THEN 'Dispute' ELSE 'Unknown' END AS payment_type_name,
    T1.fare_amount, T1.tip_amount, T1.total_amount, T1.pickup_longitude, T1.pickup_latitude, T1.dropoff_longitude, T1.dropoff_latitude,
    
    -- Derived Field: Trip Duration in Minutes
    EXTRACT(EPOCH FROM (T1.dropoff_datetime - T1.pickup_datetime)) / 60.0 AS trip_duration_minutes,
    
    -- Derived Field: Trip Speed in MPH
    CASE 
        WHEN (EXTRACT(EPOCH FROM (T1.dropoff_datetime - T1.pickup_datetime)) / 3600.0) > 0 
        THEN T1.trip_distance / (EXTRACT(EPOCH FROM (T1.dropoff_datetime - T1.pickup_datetime)) / 3600.0)
        ELSE 0 
    END AS trip_speed_mph,
    
    -- Quality Check: Coordinates within NYC approximate bounds
    (T1.pickup_longitude BETWEEN -74.25 AND -73.70 AND T1.pickup_latitude BETWEEN 40.50 AND 40.90 AND
     T1.dropoff_longitude BETWEEN -74.25 AND -73.70 AND T1.dropoff_latitude BETWEEN 40.50 AND 40.90) AS is_valid_coordinates,
    
    -- Quality Check: Fare validation (must be positive)
    (T1.fare_amount > 0 AND T1.total_amount > 0) AS is_valid_fare,
    
    -- Quality Check: Duration validation (1 minute - 180 minutes)
    (EXTRACT(EPOCH FROM (T1.dropoff_datetime - T1.pickup_datetime)) / 60.0 BETWEEN 1.0 AND 180.0) AS is_valid_duration,
    
    -- Final Valid Record Flag: Aggregate of all crucial checks
    ((T1.dropoff_datetime > T1.pickup_datetime) AND 
     (T1.fare_amount > 0) AND 
     (EXTRACT(EPOCH FROM (T1.dropoff_datetime - T1.pickup_datetime)) / 60.0 BETWEEN 1.0 AND 180.0) AND 
     (T1.pickup_longitude BETWEEN -74.25 AND -73.70 AND T1.pickup_latitude BETWEEN 40.50 AND 40.90)
    ) AS is_valid_record
FROM
    bronze.taxi_trips_raw AS T1;

-- 6. Create Data Quality Report Summary (for monitoring)
DROP TABLE IF EXISTS silver.data_quality_report;
CREATE TABLE silver.data_quality_report AS
SELECT
    DATE(silver_load_timestamp) AS report_date,
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_valid_record THEN 1 ELSE 0 END) AS fully_valid_records_count,
    (CAST(SUM(CASE WHEN is_valid_record THEN 1 ELSE 0 END) AS NUMERIC) * 100.0 / COUNT(*)) AS quality_percentage
FROM
    silver.taxi_trips_cleaned
GROUP BY
    report_date;

-- 7. Verify Silver Output
SELECT * FROM silver.data_quality_report;
SELECT silver_load_timestamp, is_valid_coordinates, is_valid_fare, is_valid_record 
FROM silver.taxi_trips_cleaned 
LIMIT 5;
