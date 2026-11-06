"""M1 - Data ingestion and cleaning.

Each invalidity criterion sets one bit of `invalid_mask`, so a single pass over
the raw data yields both the per-criterion statistics and the clean trips.
Criteria are independent: a record may count under several of them.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

KM_PER_MILE = 1.609344
MIN_LOCATION_ID, MAX_LOCATION_ID = 1, 263

# Bit position of each criterion in `invalid_mask` is its index here.
CRITERIA = (
    "null_required", "fare_le_0", "distance_le_0", "passengers_invalid",
    "dropoff_le_pickup", "location_out_of_range", "outside_file_period",
)

# Columns kept after cleaning; everything downstream is derived from these.
TRIP_COLUMNS = [
    "tpep_pickup_datetime", "PULocationID", "DOLocationID", "passenger_count",
    "trip_distance", "RatecodeID", "payment_type", "fare_amount",
]


def _criteria():
    pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    pu, do = F.col("PULocationID"), F.col("DOLocationID")
    return {
        "null_required": (pickup.isNull() | dropoff.isNull() | pu.isNull() | do.isNull()
                          | F.col("passenger_count").isNull() | F.col("trip_distance").isNull()
                          | F.col("fare_amount").isNull()),
        "fare_le_0": F.col("fare_amount") <= 0,
        "distance_le_0": F.col("trip_distance") <= 0,
        "passengers_invalid": (F.col("passenger_count") == 0) | (F.col("passenger_count") > 6),
        "dropoff_le_pickup": dropoff <= pickup,
        "location_out_of_range": (~pu.between(MIN_LOCATION_ID, MAX_LOCATION_ID)
                                  | ~do.between(MIN_LOCATION_ID, MAX_LOCATION_ID)),
        # Not in the assignment list: timestamps outside the file's own month
        # (e.g. pickups dated 2002 inside the 2024-01 file) would pollute M3/O2.
        "outside_file_period": ((F.year(pickup) != F.col("src_year"))
                                | (F.month(pickup) != F.col("src_month"))),
    }


def flag(raw: DataFrame) -> DataFrame:
    """Raw records reduced to the needed columns plus a bitmask of failed criteria."""
    exprs = _criteria()
    mask = F.lit(0)
    for bit, name in enumerate(CRITERIA):
        mask = mask.bitwiseOR(F.when(F.coalesce(exprs[name], F.lit(False)), 1 << bit).otherwise(0))
    duration = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0
    return raw.select(*TRIP_COLUMNS, "src_year", "src_month",
                      duration.alias("duration_min"), mask.alias("invalid_mask"))


def cleaning_stats(flagged: DataFrame) -> DataFrame:
    """Per-year count of records removed by each criterion."""
    mask = F.col("invalid_mask")
    aggs = [F.count(F.lit(1)).alias("records_in")]
    aggs += [F.sum((mask.bitwiseAND(1 << bit) != 0).cast("long")).alias(f"removed_{name}")
             for bit, name in enumerate(CRITERIA)]
    aggs += [F.sum((mask != 0).cast("long")).alias("removed_total")]
    return (flagged.groupBy("src_year").agg(*aggs)
            .withColumn("records_out", F.col("records_in") - F.col("removed_total")))


def clean(flagged: DataFrame) -> DataFrame:
    """Valid trips plus the derived columns every downstream operation uses."""
    pickup = F.col("tpep_pickup_datetime")
    return (flagged.filter(F.col("invalid_mask") == 0)
            .withColumnRenamed("src_year", "year")      # equal to the pickup year after cleaning
            .withColumnRenamed("src_month", "month")
            .withColumn("hour_of_day", F.hour(pickup))
            .withColumn("day_of_week", F.dayofweek(pickup))  # 1 = Sunday ... 7 = Saturday
            .withColumn("trip_km", F.col("trip_distance") * KM_PER_MILE)
            .drop("invalid_mask"))
