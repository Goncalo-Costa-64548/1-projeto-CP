"""M2 - Temporal demand analysis per (pickup_borough, day_of_week, hour_of_day)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def temporal_demand(trips: DataFrame, zones: DataFrame, opts=frozenset()) -> DataFrame:
    pu = zones.select(F.col("LocationID").alias("PULocationID"), F.col("Borough").alias("borough"))
    return (trips.join(pu, "PULocationID")
            .groupBy("borough", "day_of_week", "hour_of_day")
            .agg(F.count(F.lit(1)).alias("trip_count"),
                 F.avg("fare_amount").alias("mean_fare"),
                 F.avg("duration_min").alias("mean_duration_min"),
                 F.avg("trip_distance").alias("mean_distance_mi")))


def write(result: DataFrame, path: str):
    result.write.mode("overwrite").partitionBy("borough").parquet(path)
