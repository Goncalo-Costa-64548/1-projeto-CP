"""Summarise metrics JSON files from scripts/opt_study.sh into tables and a chart.

Usage: python scripts/summarise.py results/local/study
Writes summary.csv, per_op.csv and opt_study.pdf into the same directory.
"""
import glob
import json
import os
import sys
from collections import defaultdict

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

MB = 2 ** 20


def load_runs(study_dir):
    runs = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(study_dir, "*.json"))):
        with open(path) as f:
            d = json.load(f)
        runs[d["meta"]["tag"]].append(d)
    # Cumulative study: order configurations by number of optimisations enabled.
    return dict(sorted(runs.items(), key=lambda kv: len(kv[1][0]["meta"]["opts"])))


def _op_stages(run):
    return [s for s in run["stages"] if s["op"] != "other"]


def _cpu_s(run):
    """Executor CPU time: less sensitive than wall-clock to other load on the machine."""
    return sum(s.get("executor_cpu_ms", 0) for s in _op_stages(run)) / 1000


def _gc_s(run):
    return sum(s.get("gc_ms", 0) for s in _op_stages(run)) / 1000


def summarise(runs):
    rows, per_op = [], []
    for tag, reps in runs.items():
        walls = [r["meta"]["total_wall_s"] for r in reps]
        first = reps[0]
        stages = pd.DataFrame(first["stages"])
        stages = stages[stages["op"] != "other"]
        rows.append({
            "config": tag,
            "reps": len(reps),
            "wall_median_s": pd.Series(walls).median(),
            "wall_min_s": min(walls),
            "wall_max_s": max(walls),
            "cpu_median_s": pd.Series([_cpu_s(r) for r in reps]).median(),
            "gc_median_s": pd.Series([_gc_s(r) for r in reps]).median(),
            "stages": len(stages),
            "tasks": int(stages["num_tasks"].sum()),
            "input_MB": stages["input_bytes"].sum() / MB,
            "shuffle_write_MB": stages["shuffle_write_bytes"].sum() / MB,
            "spill_disk_MB": stages["spill_disk_bytes"].sum() / MB,
            "cache_mem_MB": sum(c["memoryUsed"] for c in first.get("cached", [])) / MB,
            "cache_disk_MB": sum(c["diskUsed"] for c in first.get("cached", [])) / MB,
        })
        for op in {o["op"] for o in first["ops"]}:
            op_walls = [next(o["wall_s"] for o in r["ops"] if o["op"] == op) for r in reps]
            o = next(o for o in first["ops"] if o["op"] == op)
            op_cpu = [sum(s.get("executor_cpu_ms", 0) for s in r["stages"] if s["op"] == op) / 1000
                      for r in reps]
            per_op.append({"config": tag, "op": op, "wall_median_s": pd.Series(op_walls).median(),
                           "cpu_median_s": pd.Series(op_cpu).median(),
                           "input_MB": o.get("input_bytes", 0) / MB,
                           "shuffle_write_MB": o.get("shuffle_write_bytes", 0) / MB,
                           "tasks": o.get("tasks", 0)})
    order = {t: i for i, t in enumerate(runs)}
    summary = pd.DataFrame(rows).sort_values("config", key=lambda s: s.map(order))
    per_op = pd.DataFrame(per_op)
    return summary, per_op


def plot(per_op, order, path):
    table = per_op.pivot(index="config", columns="op", values="wall_median_s").reindex(order)
    ax = table.plot.barh(stacked=True, figsize=(6, 2.6), width=0.7)
    ax.invert_yaxis()
    ax.set_xlabel("wall-clock time (s), median")
    ax.set_ylabel("")
    ax.legend(ncol=5, fontsize=7, loc="lower center", bbox_to_anchor=(0.5, 1.0), frameon=False)
    plt.tight_layout()
    plt.savefig(path)


def main(study_dir):
    runs = load_runs(study_dir)
    summary, per_op = summarise(runs)
    summary.round(1).to_csv(os.path.join(study_dir, "summary.csv"), index=False)
    per_op.round(2).to_csv(os.path.join(study_dir, "per_op.csv"), index=False)
    print(summary.round(1).to_string(index=False))
    print()
    print(per_op.pivot(index="config", columns="op", values="wall_median_s")
          .reindex(list(runs)).round(1).to_string())
    plot(per_op, list(runs), os.path.join(study_dir, "opt_study.pdf"))


if __name__ == "__main__":
    main(sys.argv[1])
