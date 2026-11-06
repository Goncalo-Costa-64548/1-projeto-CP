# ComPar 26/27 — Project 1: Large-Scale Analytics with Spark

PySpark pipeline over the NYC TLC Yellow Taxi trip records, used as the subject
of a performance-engineering study (profile → hypothesise → optimise → measure).

Operations implemented: **M1** (ingestion and cleaning), **M2** (temporal demand),
**M3** (zone-level window statistics), **O1** (fare anomaly detection) and
**O2** (origin-destination flows).

## Layout

```
src/main.py              entry point (CLI)
src/pipeline/loader.py   per-file read + schema enforcement
src/pipeline/m1_clean.py M1      src/pipeline/m2_temporal.py  M2
src/pipeline/m3_zones.py M3      src/pipeline/o1_fare_anomaly.py O1
src/pipeline/o2_od_flows.py O2   src/pipeline/profiling.py    stage metrics (Spark UI REST API)
src/pipeline/opts.py     optimisation switches (--opt)
scripts/                 download, local/Docker runs, studies (opt_study.sh,
                         scale_study.sh, isolate_cores.sh), analysis
                         (summarise.py, plot_scaling.py, compare_outputs.py)
docker/                  standalone Spark cluster (docker compose)
results/                 metrics JSON per run, charts
report/                  LaTeX report (ACM template)
```

## Data

Not stored in the repository. Source: NYC TLC Trip Record Data
(<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>), downloaded from
the TLC CloudFront mirror:

- trips: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- zones: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

```bash
scripts/download_data.sh 2024 2024           # one year (~600 MB)
scripts/download_data.sh 2009 2026           # full range
```

Files are stored as `$WORK/data/yellow/year=YYYY/yellow_tripdata_YYYY-MM.parquet`,
with `WORK=~/cp-proj1-work` by default. Data, outputs and Spark event logs live
outside the project folder on purpose: when the project sits in a folder synced
by iCloud/Dropbox, the sync daemons upload every output and compete for CPU
(we observed a load average above 40 on 8 cores), which ruins the timings.
Download date for the copy used in the report: **2026-10-15**.

Note: files before 2011 use lat/lon instead of `PULocationID`/`DOLocationID`; their
records are removed by the `location_out_of_range` criterion of M1.

## Setup

Requirements: Python ≥ 3.9, Java 17 or 21.

```bash
python3 -m venv ~/.venvs/cp-proj1
~/.venvs/cp-proj1/bin/pip install -r requirements.txt
```

The virtualenv must live in a path **without `:`** (a `:` in the path breaks
`PATH` and the JVM classpath).

## Running

Local mode (`local[*]`):

```bash
scripts/run_local.sh --years 2024 --fraction 0.05 --tag smoke                  # quick check
scripts/run_local.sh --years 2024 --opt none --metrics results/local/base.json  # baseline
scripts/run_local.sh --years 2024 --opt all  --metrics results/local/opt.json   # optimised
```

Optimisation study (cumulative configurations, 3 repetitions each) and its summary:

```bash
scripts/opt_study.sh 3 2024 results/local/study
~/.venvs/cp-proj1/bin/python scripts/summarise.py results/local/study
```

AQE ablation (optimised pipeline with AQE on and off, results in `results/local/aqe`):

```bash
scripts/run_local.sh --opt all --tag aqe_on --metrics results/local/aqe/aqe_on.r1.json
SPARK_CONF="--conf spark.sql.adaptive.enabled=false" \
  scripts/run_local.sh --opt all --tag aqe_off --metrics results/local/aqe/aqe_off.r1.json
```

Correctness check — the optimised pipeline must produce the same results as the baseline:

```bash
scripts/run_local.sh --fraction 0.05 --opt none --out /tmp/out_none
scripts/run_local.sh --fraction 0.05 --opt all  --out /tmp/out_all
~/.venvs/cp-proj1/bin/python scripts/compare_outputs.py /tmp/out_none /tmp/out_all
```

Docker standalone cluster:

```bash
docker compose -f docker/docker-compose.yml up -d --scale spark-worker=3
EXEC_MEM=640m scripts/run_docker.sh --years 2024 --opt o1_agg,o2_agg,m3_window,o2_ids \
  --tag w3_f1.0 --metrics results/docker/w3_f1.0.json
```

Full scalability study (1–3 workers on the full year, fractions 0.1/0.2/0.5 on
3 workers, baseline on 3 workers) and its chart:

```bash
WORKERS="1 2 3" EXEC_MEM=640m OPT=o1_agg,o2_agg,m3_window,o2_ids scripts/scale_study.sh results/docker
~/.venvs/cp-proj1/bin/python scripts/plot_scaling.py results/docker
scripts/isolate_cores.sh 6 results/docker   # control: 1 worker with 6 cores
docker compose -f docker/docker-compose.yml down
```

The cluster runs without `cache`: with 640 MB executors the 1 M-row cache
blocks do not fit and building them fails with `OutOfMemoryError`.
Workers use `WORKER_CORES` (default 2) and `WORKER_MEMORY` (default 1g).
Master UI: <http://localhost:8080>. Driver UI while running: <http://localhost:4040>.

### CLI options (`src/main.py`)

| option | default | meaning |
|---|---|---|
| `--input` | `data/yellow` (scripts pass `$WORK/data/yellow`) | directory with the monthly Parquet files |
| `--zones` | `data/taxi_zone_lookup.csv` | zone lookup table |
| `--years` | `2024` | `2024`, `2019-2024` or `2019,2021` |
| `--fraction` | `1.0` | sample fraction for smoke tests / scaling runs |
| `--ops` | `m1,m2,m3,o1,o2` | operations to run (M1 always runs) |
| `--opt` | `all` | optimisations: `all`, `none`, or a subset of `o1_agg,o2_agg,m3_window,cache,cache_nocompress,o2_ids` (see `src/pipeline/opts.py`) |
| `--out` | `output` | results directory |
| `--metrics` | – | JSON with wall-clock per op and per-stage metrics |
| `--plans` | – | directory for `explain(mode="extended")` dumps |
| `--tag` | value of `--opt` | run label |

Event logs are written to `$WORK/spark-events/`; view them with the Spark History Server.

## Reproducibility

| item | value |
|---|---|
| Spark | 4.0.1 (PySpark from PyPI; `apache/spark:4.0.1-python3` in Docker) |
| Local machine | Apple M1, 8 cores, 8 GB RAM, macOS 26.5, Java 21 |
| Local mode | `local[*]`, `spark.driver.memory=3g` (4g caused swapping on the 8 GB machine), event log enabled |
| Docker cluster | standalone, 1 master (+ driver, 1 GB) and 1–3 workers × 2 cores, one 640 MB executor per worker; Docker VM 8 CPUs / 4 GB |
| Non-default config | `--opt cache_nocompress` sets `spark.sql.inMemoryColumnarStorage.compressed=false`; everything else default (AQE on) |
| DI cluster | not used in this work |
| Data | 2024 Yellow Taxi files, downloaded 2026-10-15 |

## Report

`report/report.tex` (ACM template). Compile with any LaTeX distribution, e.g.

```bash
cd report && tectonic report.tex
```

Results used in the report: `results/local/study` and `results/local/study2`
(optimisation study, local mode), `results/local/aqe` (AQE on/off) and
`results/docker` (cluster baseline and scalability).
