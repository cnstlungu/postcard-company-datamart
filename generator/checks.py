#!/usr/bin/env python3
"""Regression checks for the data generator.

Run from the repository root, with the project's requirements installed:

    python generator/checks.py

Each check stands for a way the generated data has broken the warehouse before.
They run the generator as a subprocess so they exercise the real entry point,
including its environment-variable handling.
"""
import os
import re
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
GENERATE = ROOT / "generator" / "generate.py"
DBT_PROJECT = ROOT / "postcard_company" / "dbt_project.yml"

# Each sales feed spells the transaction date differently.
FEEDS = {
    "main.parquet": "bought_date",
    "resellers_type1.parquet": "Created Date",
    "resellers_type2.parquet": "date",
}

# The reseller feeds are a fixed 100,000 rows each and dominate the runtime, so
# the direct feed is nearly free. Keep it large enough that the final-day check
# below cannot pass or fail on a coin flip: over a two-year window, 20,000 rows
# put ~27 on any given day, where 2,000 would leave a 6% chance of none.
N_TRANSACTIONS = "20000"

failures = []


def check(name):
    """Decorator that runs a check and records a failure instead of aborting."""
    def wrap(fn):
        print(f"--- {name}")
        try:
            fn()
        except AssertionError as exc:
            failures.append(name)
            print(f"    FAIL: {exc}")
        else:
            print("    ok")
        return fn
    return wrap


def run_generator(outdir, **env_overrides):
    """Run generate.py into `outdir`; return the CompletedProcess."""
    Path(outdir).mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "INPUT_FILES_PATH": str(outdir), "N_TRANSACTIONS": N_TRANSACTIONS}
    env.update({k: str(v) for k, v in env_overrides.items()})
    return subprocess.run(
        [sys.executable, str(GENERATE)],
        env=env, cwd=ROOT, capture_output=True, text=True,
    )


def generate_into(outdir, **env_overrides):
    """Run the generator and insist it succeeded, reporting why if it did not."""
    result = run_generator(outdir, **env_overrides)
    assert result.returncode == 0, f"generator failed ({env_overrides}):\n{result.stderr}"


def sales_dates(outdir, filename):
    """Every transaction date in one feed, as ISO strings."""
    column = FEEDS[filename]
    values = [str(v) for v in pq.read_table(Path(outdir) / filename).column(column).to_pylist()]
    if filename == "resellers_type2.parquet":
        # This feed writes YYYYMMDD.
        values = [f"{v[:4]}-{v[4:6]}-{v[6:]}" for v in values]
    return values


def declared_calendar():
    """The calendar bounds as declared in the generator and in dbt_project.yml."""
    py_bounds = tuple(
        date(*map(int, m))
        for m in re.findall(
            r"^CALENDAR_(?:START|END) = date\((\d+), (\d+), (\d+)\)", GENERATE.read_text(), re.M
        )
    )
    dbt_bounds = tuple(
        date.fromisoformat(m)
        for m in re.findall(
            r"^  calendar_(?:start|end): '([\d-]+)'", DBT_PROJECT.read_text(), re.M
        )
    )
    assert len(py_bounds) == 2, f"could not read CALENDAR_START/END from {GENERATE}"
    assert len(dbt_bounds) == 2, f"could not read calendar_start/end vars from {DBT_PROJECT}"
    return py_bounds, dbt_bounds


with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)

    # The calendar the generator enforces has to be the calendar dim_date
    # materialises, or the guard protects the wrong range.
    @check("generator and dbt_project.yml declare the same calendar")
    def _():
        py_bounds, dbt_bounds = declared_calendar()
        assert py_bounds == dbt_bounds, f"generate.py says {py_bounds}, dbt_project.yml says {dbt_bounds}"

    # A window reaching outside dim_date used to generate happily; the fact rows
    # it produced then joined to no date row at all.
    @check("a window outside dim_date is refused up front")
    def _():
        result = run_generator(tmp / "reject", DATA_WINDOW_MONTHS=600, DATA_END_DATE="2026-09-14")
        assert result.returncode != 0, "generator accepted a window wider than the calendar"
        assert "dim_date" in result.stderr, f"unhelpful rejection message: {result.stderr!r}"

    # DATA_END_DATE defaults to today, so a pinned SEED alone does not pin the
    # dataset unless the end of the window is pinned too.
    @check("SEED + DATA_END_DATE reproduce a dataset exactly")
    def _():
        first, second = tmp / "repro_1", tmp / "repro_2"
        for out in (first, second):
            generate_into(out, SEED=42, DATA_END_DATE="2026-09-14")
        for filename in FEEDS:
            assert (first / filename).read_bytes() == (second / filename).read_bytes(), \
                f"{filename} differs between two identically configured runs"

    @check("a different DATA_END_DATE produces a different dataset")
    def _():
        other = tmp / "other_day"
        generate_into(other, SEED=42, DATA_END_DATE="2026-09-15")
        assert (other / "main.parquet").read_bytes() != (tmp / "repro_1" / "main.parquet").read_bytes(), \
            "DATA_END_DATE had no effect on the generated data"

    @check("every sales date lies within the requested window and the calendar")
    def _():
        (cal_start, cal_end), _dbt = declared_calendar()
        end = date(2026, 9, 14)
        start = end - timedelta(days=round(24 * 30.44))
        for filename in FEEDS:
            values = sales_dates(tmp / "repro_1", filename)
            lo, hi = min(values), max(values)
            assert lo >= start.isoformat(), f"{filename} starts at {lo}, before the window at {start}"
            assert hi <= end.isoformat(), f"{filename} ends at {hi}, after the window at {end}"
            assert lo >= cal_start.isoformat() and hi <= cal_end.isoformat(), \
                f"{filename} spans {lo}..{hi}, outside dim_date {cal_start}..{cal_end}"

    # The window used to be sampled in seconds and stop at midnight on the final
    # day, leaving that day a single reachable instant and so always empty --
    # which on the default settings means today never has any sales.
    @check("the final day of the window carries sales")
    def _():
        end = date(2026, 9, 14).isoformat()
        for filename in FEEDS:
            values = sales_dates(tmp / "repro_1", filename)
            on_last_day = sum(1 for v in values if v == end)
            assert on_last_day > 0, f"{filename} has no sales on {end}, the final day of the window"
            print(f"      {filename:26} {on_last_day:>5} rows dated {end}")

if failures:
    print(f"\n{len(failures)} check(s) failed: {', '.join(failures)}")
    sys.exit(1)
print("\nAll generator checks passed.")
