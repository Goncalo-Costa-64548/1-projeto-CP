"""O2 - Origin-destination flow analysis."""
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TOP_N = 20
ROUTE = ["PULocationID", "DOLocationID"]


def _zone_names(zones: DataFrame):
    pu = zones.select(F.col("LocationID").alias("PULocationID"),
                      F.col("Borough").alias("pu_borough"), F.col("Zone").alias("pu_zone"))
    do = zones.select(F.col("LocationID").alias("DOLocationID"),
                      F.col("Borough").alias("do_borough"), F.col("Zone").alias("do_zone"))
    return pu, do


def _with_names(df: DataFrame, zones: DataFrame) -> DataFrame:
    pu, do = _zone_names(zones)
    return df.join(F.broadcast(pu), "PULocationID").join(F.broadcast(do), "DOLocationID")


def _monthly_counts(trips: DataFrame, zones: DataFrame, opts) -> DataFrame:
    if "o2_ids" in opts:
        # Names are attached only to the final (small) outputs.
        return trips.groupBy(*ROUTE, "year", "month").agg(F.count(F.lit(1)).alias("trips"))
    if "o2_agg" in opts:
        # Count first (35M trips -> ~1M route-months), then resolve names on the
        # small result; the zone joins no longer touch every trip.
        counts = trips.groupBy(*ROUTE, "year", "month").agg(F.count(F.lit(1)).alias("trips"))
        return _with_names(counts, zones)
    pu, do = _zone_names(zones)
    return (trips.join(pu, "PULocationID").join(do, "DOLocationID")
            .groupBy(*ROUTE, "pu_zone", "pu_borough", "do_zone", "do_borough", "year", "month")
            .agg(F.count(F.lit(1)).alias("trips")))


def od_flows(trips: DataFrame, zones: DataFrame, opts=frozenset()):
    """Returns (monthly route shares, top-N routes per pickup borough, Q1/Q3 reversals,
    and the internal shares frame so `write` can release it once everything is written)."""
    by_ids = "o2_ids" in opts
    monthly = _monthly_counts(trips, zones, opts)
    w_out = Window.partitionBy("PULocationID", "year", "month")
    shares = monthly.withColumn("share", F.col("trips") / F.sum("trips").over(w_out))
    if "o2_agg" in opts or by_ids:
        # The three outputs below all derive from `shares`; persist it so the
        # trip table is aggregated once instead of once per output.
        shares = shares.persist(StorageLevel.MEMORY_AND_DISK)

    # With o2_ids the aggregations below group by integer ids only and the
    # string names are joined afterwards; otherwise names are grouping keys.
    names = [] if by_ids else ["pu_borough", "pu_zone", "do_zone"]

    # Top-N routes per pickup borough by total volume over the whole period
    # (ties broken by route id so the result is deterministic).
    totals = shares.groupBy(*ROUTE, *names).agg(F.sum("trips").alias("total_trips"))
    if by_ids:
        totals = _with_names(totals, zones).drop("do_borough")
    w_top = Window.partitionBy("pu_borough").orderBy(F.desc("total_trips"), *ROUTE)
    top = (totals.withColumn("rank", F.row_number().over(w_top))
           .filter(F.col("rank") <= TOP_N))

    # Direction of the share trend within Q1 (Jan->Mar) and within Q3 (Jul->Sep);
    # a reversal is a strictly positive trend in one and negative in the other.
    def share_at(m):
        return F.max(F.when(F.col("month") == m, F.col("share")))

    reversals = (shares.groupBy(*ROUTE, *names, "year")
                 .agg(share_at(1).alias("s_jan"), share_at(3).alias("s_mar"),
                      share_at(7).alias("s_jul"), share_at(9).alias("s_sep"))
                 .withColumn("q1_trend", F.signum(F.col("s_mar") - F.col("s_jan")))
                 .withColumn("q3_trend", F.signum(F.col("s_sep") - F.col("s_jul")))
                 .filter((F.col("q1_trend") * F.col("q3_trend")) < 0))
    if by_ids:
        reversals = _with_names(reversals, zones).drop("do_borough")
        shares_out = _with_names(shares, zones)
    else:
        shares_out = shares

    return shares_out, top, reversals, shares


def write(result, path: str):
    shares_out, top, reversals, shares = result
    shares_out.write.mode("overwrite").parquet(f"{path}/route_shares")
    top.write.mode("overwrite").parquet(f"{path}/top_routes")
    reversals.write.mode("overwrite").parquet(f"{path}/q1_q3_reversals")
    if shares.is_cached:
        shares.unpersist()
