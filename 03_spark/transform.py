from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, hour, to_timestamp, when, round, avg, count

spark = SparkSession.builder.appName("NYC Taxi Trips - Transformacion").getOrCreate()

BUCKET = "gs://big-data-nyc-taxis-project-bac9f556-1ccf-44de-801"
INPUT_PATH  = f"{BUCKET}/raw/*/*.csv"
OUTPUT_PATH = f"{BUCKET}/processed/"

df_raw = spark.read.csv(INPUT_PATH, header=True, inferSchema=False)
print(f"Registros crudos: {df_raw.count():,}")

df_clean = df_raw \
    .select(
        col("vendor_id"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("passenger_count").cast("int"),
        col("trip_distance").cast("float"),
        col("payment_type"),
        col("fare_amount").cast("float"),
        col("tip_amount").cast("float"),
        col("tolls_amount").cast("float"),
        col("total_amount").cast("float"),
        col("pickup_location_id").cast("int"),
        col("dropoff_location_id").cast("int")
    ) \
    .dropna(subset=["pickup_datetime", "dropoff_datetime", "total_amount"]) \
    .filter(col("passenger_count") > 0) \
    .filter(col("trip_distance") > 0) \
    .filter(col("fare_amount") > 0) \
    .filter(col("total_amount") > 0) \
    .filter(col("total_amount") < 1000)

df_transformed = df_clean \
    .withColumn("pickup_datetime",  to_timestamp(col("pickup_datetime"))) \
    .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"))) \
    .withColumn("year",  year(col("pickup_datetime"))) \
    .withColumn("month", month(col("pickup_datetime"))) \
    .withColumn("hour",  hour(col("pickup_datetime"))) \
    .withColumn("trip_duration_min",
        round((col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60, 2)) \
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

df_final = df_transformed \
    .filter(col("trip_duration_min") > 0) \
    .filter(col("trip_duration_min") < 300) \
    .filter(col("year").between(2016, 2023))

print(f"Registros finales: {df_final.count():,}")

df_final.write.mode("overwrite").partitionBy("year", "month").parquet(OUTPUT_PATH)
print(f"Datos guardados en: {OUTPUT_PATH}")

df_final.groupBy("year") \
    .agg(
        count("*").alias("total_viajes"),
        round(avg("fare_amount"), 2).alias("tarifa_promedio_usd"),
        round(avg("tip_percentage"), 2).alias("propina_promedio_pct")
    ).orderBy("year").show()

spark.stop()
print("Job de Spark completado!")
