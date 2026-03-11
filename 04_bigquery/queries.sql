-- ============================================================
-- NYC Taxi Trips — Queries Analíticas BigQuery
-- Tabla: project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final
-- Registros: 509,559,080 | Período: 2016-2022
-- ============================================================

-- Q1: KPIs por año
SELECT
  year,
  COUNT(*) AS total_viajes,
  ROUND(AVG(fare_amount), 2) AS tarifa_promedio_usd,
  ROUND(AVG(tip_percentage), 2) AS propina_promedio_pct,
  ROUND(AVG(trip_distance), 2) AS distancia_promedio_mi
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY year
ORDER BY year;

-- Q2: Demanda por hora del día
SELECT
  hour,
  COUNT(*) AS total_viajes,
  ROUND(AVG(fare_amount), 2) AS tarifa_promedio_usd
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY hour
ORDER BY hour;

-- Q3: Top 10 zonas de pickup
SELECT
  pickup_location_id,
  COUNT(*) AS total_viajes,
  ROUND(AVG(fare_amount), 2) AS tarifa_promedio_usd,
  ROUND(AVG(tip_percentage), 2) AS propina_promedio_pct
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY pickup_location_id
ORDER BY total_viajes DESC
LIMIT 10;

-- Q4: Distribución de métodos de pago
SELECT
  payment_type_desc,
  COUNT(*) AS total_viajes,
  ROUND(AVG(tip_percentage), 2) AS propina_promedio_pct,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS porcentaje
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY payment_type_desc
ORDER BY total_viajes DESC;

-- Q5: RANK de zonas por año (Window Function)
SELECT year, pickup_location_id, total_viajes,
  RANK() OVER (PARTITION BY year ORDER BY total_viajes DESC) AS rank_zona
FROM (
  SELECT year, pickup_location_id, COUNT(*) AS total_viajes
  FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
  GROUP BY year, pickup_location_id
)
QUALIFY rank_zona <= 3
ORDER BY year, rank_zona;

-- Q6: Análisis pre/post pandemia
SELECT
  CASE WHEN year < 2020 THEN 'Pre-pandemia (2016-2019)'
       WHEN year = 2020 THEN 'Pandemia (2020)'
       ELSE 'Post-pandemia (2021-2022)' END AS periodo,
  COUNT(*) AS total_viajes,
  ROUND(AVG(fare_amount), 2) AS tarifa_promedio,
  ROUND(AVG(trip_distance), 2) AS distancia_promedio
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY periodo
ORDER BY MIN(year);

-- Q7: Correlación distancia-tarifa por año
SELECT year,
  ROUND(CORR(trip_distance, fare_amount), 4) AS correlacion_distancia_tarifa
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY year
ORDER BY year;

-- Q8: Tendencia mensual 2016-2022
SELECT year, month, COUNT(*) AS total_viajes
FROM `project-bac9f556-1ccf-44de-801.nyc_taxis.trips_final`
GROUP BY year, month
ORDER BY year, month;
