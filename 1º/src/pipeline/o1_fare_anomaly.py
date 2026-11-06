"""O1 - Fare anomaly detection on per-km fare, per (PULocationID, hour_of_day)."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SIGMAS = 3.0
BUCKET = ["PULocationID", "hour_of_day"]


def _with_bucket_stats(trips: DataFrame, opts) -> DataFrame:
    if "o1_agg" in opts:
        # ~6k buckets: aggregate (map-side partial aggregation, tiny shuffle)
        # and broadcast the result back instead of shuffling every trip.
        stats = trips.groupBy(*BUCKET).agg(F.avg("fare_per_km").alias("bucket_mean"),
                                           F.stddev("fare_per_km").alias("bucket_std"))
        return trips.join(F.broadcast(stats), BUCKET)
    bucket = Window.partitionBy(*BUCKET)
    return (trips.withColumn("bucket_mean", F.avg("fare_per_km").over(bucket))
            .withColumn("bucket_std", F.stddev("fare_per_km").over(bucket)))


def fare_anomalies(trips: DataFrame, zones: DataFrame, opts=frozenset()) -> DataFrame:
    pu = zones.select(F.col("LocationID").alias("PULocationID"), F.col("Borough").alias("borough"))

    trips = trips.select(*BUCKET, "day_of_week", "payment_type",
                         (F.col("fare_amount") / F.col("trip_km")).alias("fare_per_km"))
    flagged = _with_bucket_stats(trips, opts).withColumn("is_anomaly", F.coalesce(
        F.col("fare_per_km") > F.col("bucket_mean") + SIGMAS * F.col("bucket_std"), F.lit(False)))

    day_type = F.when(F.col("day_of_week").isin(1, 7), "weekend").otherwise("weekday")
    return (flagged.join(pu, "PULocationID")
            .withColumn("day_type", day_type)
            .groupBy("borough", "payment_type", "day_type")
            .agg(F.count(F.lit(1)).alias("trips"),
                 F.sum(F.col("is_anomaly").cast("long")).alias("anomalies"))
            .withColumn("anomaly_rate", F.col("anomalies") / F.col("trips")))


def write(result: DataFrame, path: str):
    result.write.mode("overwrite").parquet(path)
