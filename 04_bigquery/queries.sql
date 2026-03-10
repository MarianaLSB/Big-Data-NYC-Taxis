-- ============================================
-- NYC Taxi Trips: Queries en BigQuery
-- Proyecto Final: Data Lake Migration a GCP
-- ============================================

-- ============================================
-- 1. Crear dataset
-- ============================================
CREATE SCHEMA IF NOT EXISTS `project-bac9f556-1ccf-44de-801.nyc_taxis`;

-- ============================================
-- 2. Cargar datos desde GCS (Parquet procesado)
--    Tabla particionada + clustering
-- ============================================
CREATE OR REPLACE TABLE `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY payment_type_desc, pickup_location_id
AS
SELECT
    vendor_id,
    TIMESTAMP(pickup_datetime)  AS pickup_datetime,
    TIMESTAMP(dropoff_datetime) AS dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    payment_type_desc,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    trip_duration_min,
    tip_percentage,
    year,
    month,
    hour
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_processed`;

-- ============================================
-- 3. KPIs por año (tendencia histórica)
-- ============================================
SELECT
    year,
    COUNT(*)                        AS total_viajes,
    ROUND(AVG(trip_distance), 2)    AS distancia_promedio_mi,
    ROUND(AVG(fare_amount), 2)      AS tarifa_promedio_usd,
    ROUND(AVG(tip_percentage), 2)   AS propina_promedio_pct,
    ROUND(SUM(total_amount), 2)     AS ingresos_totales_usd
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY year
ORDER BY year;

-- ============================================
-- 4. Demanda por hora del día
--    (útil para optimización de flota)
-- ============================================
SELECT
    hour,
    COUNT(*)                     AS total_viajes,
    ROUND(AVG(fare_amount), 2)   AS tarifa_promedio_usd
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY hour
ORDER BY hour;

-- ============================================
-- 5. Top 10 zonas más activas
-- ============================================
SELECT
    pickup_location_id,
    COUNT(*)                        AS total_viajes,
    ROUND(AVG(fare_amount), 2)      AS tarifa_promedio_usd,
    ROUND(AVG(tip_percentage), 2)   AS propina_promedio_pct
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY pickup_location_id
ORDER BY total_viajes DESC
LIMIT 10;

-- ============================================
-- 6. Análisis de métodos de pago
-- ============================================
SELECT
    payment_type_desc,
    COUNT(*)                        AS total_viajes,
    ROUND(AVG(tip_percentage), 2)   AS propina_promedio_pct,
    ROUND(SUM(total_amount), 2)     AS ingresos_totales_usd
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY payment_type_desc
ORDER BY total_viajes DESC;

-- ============================================
-- 7. Window Function: ranking de ingresos
--    por zona y año
-- ============================================
SELECT
    year,
    pickup_location_id,
    total_viajes,
    ingresos_totales_usd,
    RANK() OVER (
        PARTITION BY year
        ORDER BY ingresos_totales_usd DESC
    ) AS ranking_zona
FROM (
    SELECT
        year,
        pickup_location_id,
        COUNT(*)                      AS total_viajes,
        ROUND(SUM(total_amount), 2)   AS ingresos_totales_usd
    FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
    GROUP BY year, pickup_location_id
)
QUALIFY ranking_zona <= 5
ORDER BY year, ranking_zona;

-- ============================================
-- 8. Impacto de Uber/Lyft: caída en viajes
--    2016-2019 vs 2020-2023
-- ============================================
SELECT
    CASE
        WHEN year BETWEEN 2016 AND 2019 THEN 'Pre-pandemia (2016-2019)'
        WHEN year BETWEEN 2020 AND 2023 THEN 'Post-pandemia (2020-2023)'
    END AS periodo,
    COUNT(*)                        AS total_viajes,
    ROUND(AVG(fare_amount), 2)      AS tarifa_promedio_usd,
    ROUND(SUM(total_amount), 2)     AS ingresos_totales_usd
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY periodo
ORDER BY periodo;
