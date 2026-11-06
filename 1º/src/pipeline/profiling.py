"""Lightweight profiling: wall-clock per operation and stage metrics from the Spark UI REST API."""
import contextlib
import io
import json
import time
import urllib.request

from pyspark.sql import DataFrame, SparkSession


QUANTILES = "0.0,0.25,0.5,0.75,1.0"


class Profiler:
    def __init__(self, spark: SparkSession, plans_dir: str = None):
        self.spark = spark
        self.sc = spark.sparkContext
        self.plans_dir = plans_dir
        self.ops = []
        self.cached = []

    @contextlib.contextmanager
    def op(self, name: str):
        """Tag every job run inside the block with `name` and time it."""
        self.sc.setJobGroup(name, name)
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.ops.append({"op": name, "wall_s": round(time.perf_counter() - t0, 3)})
            self.sc.setJobGroup("other", "other")

    def save_plan(self, name: str, df: DataFrame):
        if not self.plans_dir:
            return
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            df.explain(mode="extended")
        with open(f"{self.plans_dir}/{name}.txt", "w") as f:
            f.write(buf.getvalue())

    # --- Spark UI REST API -------------------------------------------------
    def _get(self, path: str):
        url = f"{self.sc.uiWebUrl}/api/v1/applications/{self.sc.applicationId}{path}"
        with urllib.request.urlopen(url, timeout=30) as r:
            return json.load(r)

    def stage_metrics(self):
        """One record per completed stage, tagged with the operation (job group) that ran it."""
        stage_op = {}
        for job in self._get("/jobs"):
            for sid in job.get("stageIds", []):
                stage_op[sid] = job.get("jobGroup", "other")

        rows = []
        for s in self._get("/stages?status=complete"):
            sid, att = s["stageId"], s["attemptId"]
            row = {
                "op": stage_op.get(sid, "other"),
                "stage_id": sid,
                "attempt": att,
                "name": s.get("name", ""),
                "num_tasks": s.get("numTasks", 0),
                "executor_run_ms": s.get("executorRunTime", 0),
                "executor_cpu_ms": s.get("executorCpuTime", 0) // 1_000_000,
                "gc_ms": s.get("jvmGcTime", 0),
                "input_bytes": s.get("inputBytes", 0),
                "shuffle_read_bytes": s.get("shuffleReadBytes", 0),
                "shuffle_write_bytes": s.get("shuffleWriteBytes", 0),
                "spill_memory_bytes": s.get("memoryBytesSpilled", 0),
                "spill_disk_bytes": s.get("diskBytesSpilled", 0),
            }
            if s.get("submissionTime") and s.get("completionTime"):
                row["duration_ms"] = _ms(s["completionTime"]) - _ms(s["submissionTime"])
            # Task-duration skew: max / median executor run time. Records read
            # per task separate real skew from empty tasks (e.g. Parquet splits
            # that contain no row group).
            try:
                summary = self._get(f"/stages/{sid}/{att}/taskSummary?quantiles={QUANTILES}")
                run = summary["executorRunTime"]
                row["task_run_ms_q"] = run
                row["task_median_ms"], row["task_max_ms"] = run[2], run[4]
                row["skew_ratio"] = round(run[4] / run[2], 2) if run[2] else None
                row["task_records_read_q"] = summary.get("inputMetrics", {}).get("recordsRead")
            except (OSError, KeyError, ValueError):
                pass
            rows.append(row)
        return sorted(rows, key=lambda r: r["stage_id"])

    def report(self, meta: dict):
        stages = self.stage_metrics()
        totals = {}
        for r in stages:
            t = totals.setdefault(r["op"], {"stages": 0, "tasks": 0, "shuffle_read_bytes": 0,
                                            "shuffle_write_bytes": 0, "input_bytes": 0})
            t["stages"] += 1
            t["tasks"] += r["num_tasks"]
            for k in ("shuffle_read_bytes", "shuffle_write_bytes", "input_bytes"):
                t[k] += r[k]
        for o in self.ops:
            o.update(totals.get(o["op"], {}))
        return {"meta": meta, "ops": self.ops, "stages": stages, "cached": self.cached}

    def snapshot_storage(self):
        """Record the size of persisted datasets (call while they are still cached)."""
        for rdd in self._get("/storage/rdd"):
            self.cached.append({k: rdd.get(k) for k in (
                "name", "numPartitions", "numCachedPartitions", "memoryUsed", "diskUsed")})


def _ms(ts: str) -> int:
    # Spark UI format: 2026-10-15T10:00:00.123GMT
    from datetime import datetime, timezone
    dt = datetime.strptime(ts.replace("GMT", ""), "%Y-%m-%dT%H:%M:%S.%f")
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
