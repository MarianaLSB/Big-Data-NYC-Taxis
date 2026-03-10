-- ============================================
-- NYC Taxi Trips: Esquema en Hive
-- Proyecto Final: Data Lake Migration a GCP
-- ============================================

-- Crear base de datos
CREATE DATABASE IF NOT EXISTS nyc_taxis;
USE nyc_taxis;

-- ============================================
-- TABLA 1: External Table (datos crudos en GCS)
-- Simula datos en sistema On-premise/HDFS
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS trips_raw (
    vendor_id           STRING,
    pickup_datetime     STRING,
    dropoff_datetime    STRING,
    passenger_count     INT,
    trip_distance       FLOAT,
    pickup_location_id  INT,
    dropoff_location_id INT,
    payment_type        STRING,
    fare_amount         FLOAT,
    tip_amount          FLOAT,
    tolls_amount        FLOAT,
    total_amount        FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION 'gs://big-data-nyc-taxis-project-bac9f556-1ccf-44de-801/raw/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Verificar ingesta
SELECT COUNT(*) FROM trips_raw;
SELECT * FROM trips_raw LIMIT 5;

-- ============================================
-- TABLA 2: Tabla Particionada (por año)
-- Optimizada para queries analíticas
-- ============================================
CREATE TABLE IF NOT EXISTS trips_partitioned (
    vendor_id           STRING,
    pickup_datetime     STRING,
    dropoff_datetime    STRING,
    passenger_count     INT,
    trip_distance       FLOAT,
    pickup_location_id  INT,
    dropoff_location_id INT,
    payment_type        STRING,
    fare_amount         FLOAT,
    tip_amount          FLOAT,
    tolls_amount        FLOAT,
    total_amount        FLOAT
)
PARTITIONED BY (year INT)
STORED AS PARQUET;

-- Habilitar particionamiento dinámico
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Cargar datos particionados por año
INSERT INTO TABLE trips_partitioned PARTITION (year)
SELECT
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    YEAR(TO_DATE(pickup_datetime)) AS year
FROM trips_raw
WHERE pickup_datetime IS NOT NULL;

-- Verificar particiones
SHOW PARTITIONS trips_partitioned;
SELECT year, COUNT(*) as total_viajes FROM trips_partitioned GROUP BY year ORDER BY year;
