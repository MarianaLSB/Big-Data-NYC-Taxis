# ============================================
# NYC Taxi Trips: Transformación con PySpark
# Proyecto Final: Data Lake Migration a GCP
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, hour, to_timestamp,
    when, round, avg, count, sum as spark_sum
)

# ============================================
# 1. Inicializar SparkSession
# ============================================
spark = SparkSession.builder \
    .appName("NYC Taxi Trips - Transformacion") \
    .getOrCreate()

# Variables
BUCKET = "gs://big-data-nyc-taxis-project-bac9f556-1ccf-44de-801"
INPUT_PATH  = f"{BUCKET}/raw/"
OUTPUT_PATH = f"{BUCKET}/processed/"

print("✅ SparkSession iniciada")

# ============================================
# 2. Leer datos crudos desde GCS
# ============================================
df_raw = spark.read.csv(
    INPUT_PATH,
    header=True,
    inferSchema=True
)

print(f"📊 Registros crudos: {df_raw.count():,}")
df_raw.printSchema()

# ============================================
# 3. Limpieza de datos
# ============================================
df_clean = df_raw \
    .dropna(subset=["pickup_datetime", "dropoff_datetime", "total_amount"]) \
    .filter(col("passenger_count") > 0) \
    .filter(col("trip_distance") > 0) \
    .filter(col("fare_amount") > 0) \
    .filter(col("total_amount") > 0) \
    .filter(col("total_amount") < 1000)  # Eliminar outliers extremos

print(f"✅ Registros después de limpieza: {df_clean.count():,}")

# ============================================
# 4. Transformaciones y Feature Engineering
# ============================================
df_transformed = df_clean \
    .withColumn("pickup_datetime",  to_timestamp(col("pickup_datetime"))) \
    .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"))) \
    .withColumn("year",  year(col("pickup_datetime"))) \
    .withColumn("month", month(col("pickup_datetime"))) \
    .withColumn("hour",  hour(col("pickup_datetime"))) \
    .withColumn("trip_duration_min",
        round((col("dropoff_datetime").cast("long") -
               col("pickup_datetime").cast("long")) / 60, 2)) \
    .withColumn("payment_type_desc",
        when(col("payment_type") == "1", "Credit Card")
        .when(col("payment_type") == "2", "Cash")
        .when(col("payment_type") == "3", "No Charge")
        .when(col("payment_type") == "4", "Dispute")
        .otherwise("Unknown")) \
    .withColumn("tip_percentage",
        when(col("fare_amount") > 0,
             round((col("tip_amount") / col("fare_amount")) * 100, 2))
        .otherwise(0.0))

print("✅ Transformaciones aplicadas")

# ============================================
# 5. Filtrar registros válidos post-transformación
# ============================================
df_final = df_transformed \
    .filter(col("trip_duration_min") > 0) \
    .filter(col("trip_duration_min") < 300)  # Máximo 5 horas

print(f"✅ Registros finales: {df_final.count():,}")

# ============================================
# 6. Guardar resultado en Parquet
# ============================================
df_final.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(OUTPUT_PATH)

print(f"✅ Datos guardados en Parquet: {OUTPUT_PATH}")

# ============================================
# 7. Agregaciones para análisis (KPIs)
# ============================================
print("\n📈 KPIs por año:")
df_final.groupBy("year") \
    .agg(
        count("*").alias("total_viajes"),
        round(avg("trip_distance"), 2).alias("distancia_promedio_mi"),
        round(avg("fare_amount"), 2).alias("tarifa_promedio_usd"),
        round(avg("tip_percentage"), 2).alias("propina_promedio_pct"),
        round(spark_sum("total_amount"), 2).alias("ingresos_totales_usd")
    ) \
    .orderBy("year") \
    .show()

print("\n📈 Top 10 zonas de recogida:")
df_final.groupBy("pickup_location_id") \
    .agg(count("*").alias("total_viajes")) \
    .orderBy(col("total_viajes").desc()) \
    .limit(10) \
    .show()

spark.stop()
print("✅ Job de Spark completado")
