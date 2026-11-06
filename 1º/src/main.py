"""NYC Yellow Taxi analytics pipeline (ComPar 26/27, Project 1).

Example:
    spark-submit src/main.py --input data/yellow --zones data/taxi_zone_lookup.csv \
        --years 2024 --fraction 0.1 --opt all --metrics results/local/opt_f0.1.json
"""
import argparse
import csv
import json
import os
import platform
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from pipeline import loader, m1_clean, m2_temporal, m3_zones, o1_fare_anomaly, o2_od_flows, opts
from pipeline.profiling import Profiler

# name -> (compute(trips, zones, opts), write(result, path))
OPS = {
    "m2": (m2_temporal.temporal_demand, m2_temporal.write),
    "m3": (m3_zones.zone_stats, m3_zones.write),
    "o1": (o1_fare_anomaly.fare_anomalies, o1_fare_anomaly.write),
    "o2": (o2_od_flows.od_flows, o2_od_flows.write),
}


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", default="data/yellow", help="directory with yellow_tripdata_*.parquet")
    p.add_argument("--zones", default="data/taxi_zone_lookup.csv")
    p.add_argument("--years", default="2024", help="e.g. 2024, 2019-2024, 2019,2021")
    p.add_argument("--fraction", type=float, default=1.0, help="sample fraction of records (0,1]")
    p.add_argument("--ops", default="m1,m2,m3,o1,o2", help="comma-separated operations to run")
    p.add_argument("--opt", default="all", help=f"optimisations: all | none | subset of {','.join(opts.ALL)}")
    p.add_argument("--out", default="output", help="output directory for results")
    p.add_argument("--metrics", default=None, help="JSON file for profiling metrics")
    p.add_argument("--plans", default=None, help="directory to dump explain(extended) plans")
    p.add_argument("--tag", default=None, help="label for this run in the metrics (default: --opt)")
    return p.parse_args()


def main():
    args = parse_args()
    enabled = opts.parse(args.opt)
    tag = args.tag or args.opt
    spark = SparkSession.builder.appName(f"nyc-taxi-{tag}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.plans:
        os.makedirs(args.plans, exist_ok=True)
    os.makedirs(args.out, exist_ok=True)
    prof = Profiler(spark, args.plans)

    years = loader.parse_years(args.years)
    ops = [o.strip() for o in args.ops.split(",") if o.strip()]
    zones = loader.load_zones(spark, args.zones)
    raw = loader.load_trips(spark, args.input, years, args.fraction)

    flagged = m1_clean.flag(raw)
    if "cache_nocompress" in enabled:
        spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "false")
    if "cache" in enabled:
        flagged = flagged.persist(StorageLevel.MEMORY_AND_DISK)
    trips = m1_clean.clean(flagged)

    t0 = time.perf_counter()
    # M1 always runs: with `cache` it is the single pass that materialises `flagged`.
    with prof.op("m1"):
        stats = m1_clean.cleaning_stats(flagged)
        prof.save_plan("m1", stats)
        # One row per year: sort on the driver rather than with a Spark range shuffle.
        rows = sorted(stats.collect(), key=lambda r: r["src_year"])
        with open(f"{args.out}/m1_cleaning_stats.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(stats.columns)
            writer.writerows(rows)
    if "cache" in enabled:
        prof.snapshot_storage()

    for name in ops:
        if name not in OPS:
            continue
        compute, write = OPS[name]
        with prof.op(name):
            result = compute(trips, zones, enabled)
            for i, df in enumerate(result if isinstance(result, tuple) else (result,)):
                prof.save_plan(f"{name}_{i}", df)
            write(result, f"{args.out}/{name}")
    total = time.perf_counter() - t0

    if args.metrics:
        os.makedirs(os.path.dirname(args.metrics) or ".", exist_ok=True)
        conf = {k: v for k, v in spark.sparkContext.getConf().getAll()
                if k.startswith("spark.") and "id" not in k.lower() and "host" not in k}
        meta = {"tag": tag, "opts": sorted(enabled), "years": years, "fraction": args.fraction,
                "ops": ops, "total_wall_s": round(total, 3), "spark_version": spark.version,
                "python": platform.python_version(), "machine": platform.platform(),
                "default_parallelism": spark.sparkContext.defaultParallelism, "conf": conf}
        with open(args.metrics, "w") as f:
            json.dump(prof.report(meta), f, indent=2)

    print(f"[{tag}] done in {total:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
