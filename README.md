# NYC Taxi Trips — Data Lake Migration

**Proyecto Final — Análisis de Grandes Volúmenes de Datos**  
Universidad Panamericana | 2026

## Equipo
- Ferrán González Farré
- Mariana López Santibáñez Ballesteros
- Ricardo Alfonso Zepahua Enríquez

## Caso de Negocio
TaxiCorp NYC migra su infraestructura Hadoop On-Premise a Google Cloud Platform para analizar el impacto del ridesharing (Uber/Lyft) en la demanda de taxis amarillos 2016–2022.

## Dataset
- **Fuente:** `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_*`
- **Período:** 2016–2022 (7 años)
- **Volumen raw:** 50.9 GiB
- **Registros procesados:** 509,559,080

## Arquitectura del Pipeline
```
BigQuery Public Data → GCS (raw CSV) → Hive (DDL) → Spark (ETL) → GCS (Parquet) → BigQuery → Looker Studio
```

## Resultados Clave
| Año | Viajes | Tarifa Promedio |
|-----|--------|-----------------|
| 2016 | 130,077,965 | $13.01 |
| 2017 | 112,342,924 | $12.92 |
| 2018 | 100,884,272 | $12.95 |
| 2019 | 81,527,482  | $13.15 |
| 2020 | 22,905,569  | $11.95 |
| 2021 | 28,193,761  | $12.87 |
| 2022 | 33,627,107  | $13.71 |

> Caída del **82%** en viajes entre 2016 y 2020 (pandemia + Uber/Lyft)

## Infraestructura GCP
- **Proyecto:** `project-bac9f556-1ccf-44de-801`
- **Bucket:** `gs://big-data-nyc-taxis-project-bac9f556-1ccf-44de-801`
- **Clúster Dataproc:** `cluster-c814` (us-central1)
- **Dataset BigQuery:** `nyc_taxis.trips_final`

## Estructura del Repo
```
├── 01_ingesta/      # Script de exportación BigQuery → GCS
├── 02_hive/         # DDL schema.hql
├── 03_spark/        # ETL transform.py
├── 04_bigquery/     # 8 queries analíticas
└── docs/            # Arquitectura y evidencias
```
