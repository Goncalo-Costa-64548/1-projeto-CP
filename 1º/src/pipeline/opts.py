"""Optimisation switches.

Each optimisation from the report's optimisation study can be toggled
independently, so every before/after measurement runs on the same code base:

    --opt none           baseline
    --opt cache,o1_agg   a subset
    --opt all            fully optimised pipeline (default)
"""

# In the order they were applied in the optimisation study.
ALL = (
    "o1_agg",     # O1 bucket stats via groupBy + broadcast join instead of a window
    "o2_agg",     # O2 aggregates before the zone joins and reuses the monthly shares
    "m3_window",  # M3 borough rolling totals via a range window instead of a self-join
    "cache",      # one scan of the raw data; flagged + pruned trips persisted
    "cache_nocompress",  # build the cache without columnar compression (cheaper to encode)
    "o2_ids",     # O2 groups by integer zone ids; names joined on the small outputs
)


def parse(spec: str) -> frozenset:
    spec = spec.strip().lower()
    if spec == "all":
        return frozenset(ALL)
    if spec in ("", "none"):
        return frozenset()
    chosen = {s.strip() for s in spec.split(",") if s.strip()}
    unknown = chosen - set(ALL)
    if unknown:
        raise ValueError(f"unknown optimisations: {sorted(unknown)}; valid: {ALL}")
    return frozenset(chosen)
