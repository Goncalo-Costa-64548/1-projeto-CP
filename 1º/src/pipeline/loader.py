"""Loading of the raw TLC Parquet files and of the zone lookup table."""
import glob
import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Target schema enforced on every monthly file (M1). Monthly files disagree on
# physical types (e.g. passenger_count is int64 in some months, double in
# others), so each file is read separately and cast before the union.
TRIP_SCHEMA = [
    # Wall-clock local time without zone, as stored in the TLC files; keeping
    # NTZ avoids a per-row cast and DST shifts from the session time zone.
    ("tpep_pickup_datetime", T.TimestampNTZType()),
    ("tpep_dropoff_datetime", T.TimestampNTZType()),
    ("PULocationID", T.IntegerType()),
    ("DOLocationID", T.IntegerType()),
    ("passenger_count", T.IntegerType()),
    ("trip_distance", T.DoubleType()),
    ("RatecodeID", T.IntegerType()),
    ("payment_type", T.IntegerType()),
    ("fare_amount", T.DoubleType()),
    ("tip_amount", T.DoubleType()),
    ("total_amount", T.DoubleType()),
]

# Column names used by older files (2009-2010) for the same fields.
LEGACY_NAMES = {
    "Trip_Pickup_DateTime": "tpep_pickup_datetime",
    "pickup_datetime": "tpep_pickup_datetime",
    "Trip_Dropoff_DateTime": "tpep_dropoff_datetime",
    "dropoff_datetime": "tpep_dropoff_datetime",
    "Passenger_Count": "passenger_count",
    "Trip_Distance": "trip_distance",
    "Rate_Code": "RatecodeID",
    "rate_code": "RatecodeID",
    "Payment_Type": "payment_type",
    "Fare_Amt": "fare_amount",
    "Tip_Amt": "tip_amount",
    "Total_Amt": "total_amount",
}


def parse_years(spec: str):
    """'2024' -> [2024]; '2019-2024' -> [2019..2024]; '2019,2021' -> [2019, 2021]."""
    years = []
    for part in spec.split(","):
        if "-" in part:
            a, b = part.split("-")
            years.extend(range(int(a), int(b) + 1))
        else:
            years.append(int(part))
    return years


def trip_files(input_dir: str, years):
    files = []
    for y in years:
        files += sorted(glob.glob(os.path.join(input_dir, f"year={y}", "yellow_tripdata_*.parquet")))
        files += sorted(glob.glob(os.path.join(input_dir, f"yellow_tripdata_{y}-*.parquet")))
    if not files:
        raise FileNotFoundError(f"no yellow_tripdata files for {years} under {input_dir}")
    return files


def _read_one(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.parquet(path)
    for old, new in LEGACY_NAMES.items():
        if old in df.columns and new not in df.columns:
            df = df.withColumnRenamed(old, new)
    cols = []
    for name, dtype in TRIP_SCHEMA:
        c = F.col(name) if name in df.columns else F.lit(None)
        cols.append(c.cast(dtype).alias(name))
    # Year/month the file belongs to, used for per-year statistics and to
    # discard records whose timestamps fall outside the file's period.
    stem = os.path.basename(path).replace("yellow_tripdata_", "").replace(".parquet", "")
    y, m = stem.split("-")
    cols += [F.lit(int(y)).alias("src_year"), F.lit(int(m)).alias("src_month")]
    return df.select(*cols)


def load_trips(spark: SparkSession, input_dir: str, years, fraction: float = 1.0, seed: int = 42) -> DataFrame:
    parts = [_read_one(spark, p) for p in trip_files(input_dir, years)]
    df = reduce(DataFrame.unionByName, parts)
    if fraction < 1.0:
        df = df.sample(fraction=fraction, seed=seed)
    return df


def load_zones(spark: SparkSession, path: str) -> DataFrame:
    schema = "LocationID INT, Borough STRING, Zone STRING, service_zone STRING"
    return spark.read.csv(path, header=True, schema=schema)
