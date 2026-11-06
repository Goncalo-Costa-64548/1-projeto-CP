"""M3 - Zone-level monthly statistics with window functions."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DEVIATION_SIGMAS = 1.5
ROLLING_MONTHS = 3


def zone_stats(trips: DataFrame, zones: DataFrame, opts=frozenset()) -> DataFrame:
    pu = zones.select(F.col("LocationID").alias("PULocationID"),
                      F.col("Borough").alias("borough"), F.col("Zone").alias("zone"))

    monthly = (trips.join(pu, "PULocationID")
               .groupBy("borough", "PULocationID", "zone", "year", "month")
               .agg(F.count(F.lit(1)).alias("trip_count"),
                    F.avg("fare_amount").alias("mean_fare"),
                    F.stddev("fare_amount").alias("std_fare"),
                    F.avg("trip_distance").alias("mean_distance_mi"),
                    F.stddev("trip_distance").alias("std_distance_mi"))
               # Contiguous month index so range windows handle missing months.
               .withColumn("month_idx", F.col("year") * 12 + F.col("month") - 1))

    if "m3_window" in opts:
        # A range frame over month_idx includes every zone row of the borough in
        # the last 3 months, i.e. the borough's rolling total, with no self-join.
        w_b_roll = (Window.partitionBy("borough").orderBy("month_idx")
                    .rangeBetween(-(ROLLING_MONTHS - 1), 0))
        monthly = monthly.withColumn("borough_trips_roll3", F.sum("trip_count").over(w_b_roll))
    else:
        borough_month = (monthly.groupBy("borough", "month_idx")
                         .agg(F.sum("trip_count").alias("borough_trips")))
        w_b_roll = (Window.partitionBy("borough").orderBy("month_idx")
                    .rangeBetween(-(ROLLING_MONTHS - 1), 0))
        borough_month = (borough_month
                         .withColumn("borough_trips_roll3", F.sum("borough_trips").over(w_b_roll))
                         .drop("borough_trips"))
        monthly = monthly.join(borough_month, ["borough", "month_idx"])

    w_rank = Window.partitionBy("borough", "month_idx").orderBy(F.desc("trip_count"))
    w_roll = Window.partitionBy("PULocationID").orderBy("month_idx").rangeBetween(-(ROLLING_MONTHS - 1), 0)
    # Previous months only, so the current month is compared against its history.
    w_hist = Window.partitionBy("PULocationID").orderBy("month_idx").rangeBetween(-ROLLING_MONTHS, -1)

    return (monthly
            .withColumn("volume_rank", F.rank().over(w_rank))
            .withColumn("zone_trips_roll3", F.sum("trip_count").over(w_roll))
            .withColumn("roll3_share", F.col("zone_trips_roll3") / F.col("borough_trips_roll3"))
            .withColumn("hist_mean", F.avg("trip_count").over(w_hist))
            .withColumn("hist_std", F.stddev("trip_count").over(w_hist))
            .withColumn("volume_anomaly",
                        F.coalesce(F.abs(F.col("trip_count") - F.col("hist_mean"))
                                   > DEVIATION_SIGMAS * F.col("hist_std"), F.lit(False))))


def write(result: DataFrame, path: str):
    result.write.mode("overwrite").parquet(path)
