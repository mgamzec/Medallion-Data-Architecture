-- FILE: 1_bronze_layer.sql
--------------------------------------------------------------------------------
-- BRONZE LAYER: Raw Data Ingestion
-- Purpose: To store data exactly as it is received from the source, 
-- including all raw columns and metadata for traceability.
--------------------------------------------------------------------------------

-- 1. Create Schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- 2. Drop existing table for a fresh start (Development only)
DROP TABLE IF EXISTS bronze.taxi_trips_raw;

-- 3. Create Raw Data Table
CREATE TABLE bronze.taxi_trips_raw (
    vendor_id TEXT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance NUMERIC,
    rate_code_id INTEGER,
    store_and_fwd_flag TEXT,
    pickup_longitude NUMERIC,
    pickup_latitude NUMERIC,
    dropoff_longitude NUMERIC,
    dropoff_latitude NUMERIC,
    payment_type INTEGER,
    fare_amount NUMERIC,
    extra NUMERIC,
    mta_tax NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    improvement_surcharge NUMERIC,
    total_amount NUMERIC,
    load_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(), -- METADATA: Ingestion time
    source_file TEXT -- METADATA: Source file name
);

-- 4. Create Index on load timestamp for efficient time-based auditing
CREATE INDEX IF NOT EXISTS idx_bronze_load_timestamp
ON bronze.taxi_trips_raw (load_timestamp);

-- 5. Insert Sample Data
-- Includes two records with intentional quality issues (negative fare and invalid coordinates)
INSERT INTO bronze.taxi_trips_raw (
    vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, rate_code_id, store_and_fwd_flag,
    pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax,
    tip_amount, tolls_amount, improvement_surcharge, total_amount, source_file
)
VALUES
('1', '2023-01-01 10:00:00', '2023-01-01 10:15:00', 1, 1.5, 1, 'N', -73.985, 40.758, -73.978, 40.765, 1, 10.0, 0.5, 0.5, 2.0, 0.0, 0.3, 13.3, 'trip_data_1.csv'),
('2', '2023-01-01 11:00:00', '2023-01-01 11:30:00', 2, 5.0, 1, 'N', -74.005, 40.712, -73.999, 40.751, 2, 25.0, 0.5, 0.5, 0.0, 0.0, 0.3, 26.3, 'trip_data_1.csv'),
('1', '2023-01-01 15:00:00', '2023-01-01 15:15:00', 1, 1.5, 1, 'N', -73.985, 40.758, -73.978, 40.765, 1, -10.0, 0.5, 0.5, 2.0, 0.0, 0.3, -6.7, 'trip_data_1.csv'), -- BAD: Negative Fare
('4', '2023-01-01 16:00:00', '2023-01-01 16:30:00', 2, 5.0, 1, 'N', 0.0, 0.0, 0.0, 0.0, 2, 25.0, 0.5, 0.5, 0.0, 0.0, 0.3, 26.3, 'trip_data_1.csv'); -- BAD: Invalid Coordinates

-- 6. Verify Bronze Load
SELECT COUNT(*) AS total_records FROM bronze.taxi_trips_raw;
SELECT * FROM bronze.taxi_trips_raw LIMIT 5;
