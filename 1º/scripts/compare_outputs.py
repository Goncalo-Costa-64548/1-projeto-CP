"""Check that two pipeline runs produced the same results (up to float tolerance).

Usage: python scripts/compare_outputs.py OUT_DIR_A OUT_DIR_B
"""
import os
import sys

import pandas as pd

TABLES = ["m2", "m3", "o1", "o2/route_shares", "o2/top_routes", "o2/q1_q3_reversals"]


def load(path):
    df = pd.read_parquet(path)
    if "borough" in df.columns:  # partition column comes back as category
        df["borough"] = df["borough"].astype(str)
    return df


def canonical(df):
    df = df[sorted(df.columns)]
    return df.sort_values(list(df.columns)).reset_index(drop=True)


def main(a, b):
    ok = True
    stats_a = pd.read_csv(os.path.join(a, "m1_cleaning_stats.csv"))
    stats_b = pd.read_csv(os.path.join(b, "m1_cleaning_stats.csv"))
    same = stats_a.equals(stats_b)
    ok &= same
    print(f"{'ok  ' if same else 'DIFF'} m1_cleaning_stats")
    for t in TABLES:
        da, db = load(os.path.join(a, t)), load(os.path.join(b, t))
        try:
            pd.testing.assert_frame_equal(canonical(da), canonical(db), check_dtype=False, rtol=1e-9)
            print(f"ok   {t} ({len(da)} rows)")
        except AssertionError as e:
            ok = False
            print(f"DIFF {t}: {str(e).splitlines()[0]}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
