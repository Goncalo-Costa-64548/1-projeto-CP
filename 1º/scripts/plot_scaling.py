"""Scalability tables and chart from scripts/scale_study.sh metrics.

Usage: python scripts/plot_scaling.py results/docker
Writes scaling.csv and scaling.pdf (speedup vs workers, time vs data fraction).
"""
import glob
import json
import os
import re
import sys

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

TAG = re.compile(r"w(\d+)_f([\d.]+)(_baseline)?$")


def load(study_dir):
    rows = []
    for path in glob.glob(os.path.join(study_dir, "w*.json")):
        with open(path) as f:
            d = json.load(f)
        m = TAG.match(d["meta"]["tag"])
        if not m:
            continue
        ops = {o["op"]: o["wall_s"] for o in d["ops"]}
        rows.append({"workers": int(m.group(1)), "fraction": float(m.group(2)),
                     "config": "baseline" if m.group(3) else "optimised",
                     "wall_s": d["meta"]["total_wall_s"], **ops})
    return pd.DataFrame(rows).sort_values(["config", "workers", "fraction"])


def main(study_dir):
    df = load(study_dir)
    opt = df[df.config == "optimised"]
    size = opt[opt.fraction == 1.0].sort_values("workers").copy()
    t1 = size.iloc[0]["wall_s"] * size.iloc[0]["workers"]  # normalise to 1 worker
    size["speedup"] = t1 / size["wall_s"]
    size["efficiency"] = size["speedup"] / size["workers"]
    scale = opt[opt.workers == opt.workers.max()].sort_values("fraction").copy()
    scale["time_ratio"] = scale["wall_s"] / scale["wall_s"].max()

    df.round(2).to_csv(os.path.join(study_dir, "scaling.csv"), index=False)
    print(df.round(1).to_string(index=False))
    print(size[["workers", "wall_s", "speedup", "efficiency"]].round(2).to_string(index=False))
    print(scale[["fraction", "wall_s", "time_ratio"]].round(2).to_string(index=False))

    fig, (a, b) = plt.subplots(1, 2, figsize=(6.6, 2.4))
    a.plot(size["workers"], size["speedup"], "o-", label="measured")
    a.plot(size["workers"], size["workers"], "--", color="grey", label="ideal")
    a.set_xlabel("workers (2 cores each)")
    a.set_ylabel("speedup vs 1 worker")
    a.set_xticks(size["workers"])
    a.legend(fontsize=7, frameon=False)
    b.plot(scale["fraction"], scale["wall_s"], "o-", label="measured")
    b.plot(scale["fraction"], scale["fraction"] * scale["wall_s"].max(), "--", color="grey",
           label="linear")
    b.set_xlabel("dataset fraction")
    b.set_ylabel("wall-clock (s)")
    b.legend(fontsize=7, frameon=False)
    plt.tight_layout()
    plt.savefig(os.path.join(study_dir, "scaling.pdf"))


if __name__ == "__main__":
    main(sys.argv[1])
