#!/usr/bin/env python3
"""Run the silver transforms locally against the committed samples.

No Databricks needed: because pipelines/silver.py is a thin declarative
shell over src/silver_transforms.py, this script exercises the EXACT logic
the pipeline runs — on a laptop, in ~a minute, against data/samples/. Use it
before every pipeline change; it catches expression bugs (bad regex, null
logic, type mismatches) without spending a pipeline update on them.

  python3 tools/smoke_test_silver_logic.py          # needs pyspark + Java 17

What it emulates and what it can't:
  * bronze  -> read samples with primitivesAsString (≈ Auto Loader's
    inferColumnTypes=false: everything a string, arrays preserved)
  * the CDC step -> a latest-wins window (create_auto_cdc_flow itself is
    Databricks-only)
  * expectations -> plain filters/assertions here; metrics/enforcement are
    pipeline features
"""

import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.config import FX_TO_USD, SALARY_ANNUAL_USD_MAX, SALARY_ANNUAL_USD_MIN
from src.silver_transforms import (
    enrich_postings,
    salary_base_from,
    skills_from,
    typed_postings,
)

CANON_EMPLOYMENT = {"full_time", "part_time", "contract", "temporary",
                    "internship", "apprenticeship", "freelance", "seasonal",
                    "volunteer", "other", "unknown"}
CANON_SENIORITY = {"internship", "entry", "mid", "senior", "lead", "manager",
                   "director", "executive", "unknown"}

failures = []


def check(name, cond, detail=""):
    status = "ok " if cond else "FAIL"
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
    if not cond:
        failures.append(name)


def main():
    spark = (SparkSession.builder.master("local[2]")
             .appName("silver-smoke")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    samples = os.path.join(REPO, "data", "samples", "*.ndjson")
    bronze_like = (spark.read
                   .option("primitivesAsString", "true")
                   .json(samples))
    n_bronze = bronze_like.count()
    print(f"\nloaded {n_bronze} sample rows, {len(bronze_like.columns)} inferred columns")

    # ---- typed ----------------------------------------------------------
    typed = typed_postings(bronze_like).cache()
    n_typed = typed.count()
    print(f"\n== postings_typed: {n_typed} rows ==")
    check("no rows lost in typing", n_typed == n_bronze, f"{n_typed} vs {n_bronze}")
    check("posting_key never null (fail-expectation contract)",
          typed.filter("posting_key IS NULL OR board IS NULL OR source_id IS NULL "
                       "OR exported_at IS NULL").count() == 0)
    check("is_remote never null (three-valued-logic guard)",
          typed.filter("is_remote IS NULL").count() == 0)
    bad_emp = typed.filter(~F.col("employment_type").isin(*CANON_EMPLOYMENT)).count()
    check("employment_type always canonical", bad_emp == 0, f"{bad_emp} rows outside")
    bad_sen = typed.filter(~F.col("seniority").isin(*CANON_SENIORITY)).count()
    check("seniority always canonical", bad_sen == 0, f"{bad_sen} rows outside")

    print("\nemployment_type distribution:")
    typed.groupBy("employment_type").count().orderBy(F.desc("count")).show(12, False)
    print("seniority x source:")
    typed.groupBy("seniority", "seniority_source").count().orderBy(F.desc("count")).show(10, False)
    print("location parser coverage:")
    typed.groupBy("location_quality").count().orderBy(F.desc("count")).show(5, False)
    print("status:")
    typed.groupBy("status").count().orderBy(F.desc("count")).show(8, False)

    # ---- CDC emulation: latest wins per (board, source_id) --------------
    rn = Window.partitionBy("board", "source_id").orderBy(F.col("exported_at").desc())
    current = (typed.withColumn("_rn", F.row_number().over(rn))
                    .filter("_rn = 1").drop("_rn"))

    # ---- enriched postings ---------------------------------------------
    postings = enrich_postings(current).cache()
    print(f"== postings (enriched): {postings.count()} rows ==")
    xb = postings.filter("is_cross_board_duplicate").cache()
    n_groups = xb.select("dup_key").distinct().count()
    print(f"cross-board duplicate groups IN SAMPLES: {n_groups} "
          f"(full data measured 303 computer<->solar + 198 coffee<->computer)")
    if n_groups:
        xb.groupBy("dup_key").agg(
            F.array_sort(F.collect_set("board")).alias("boards"),
            F.first("title").alias("title")).show(5, 60)
    check("exactly one canonical row per dup group",
          postings.groupBy("dup_key")
                  .agg(F.sum(F.col("is_canonical").cast("int")).alias("c"))
                  .filter("c <> 1").count() == 0)

    # ---- skills ---------------------------------------------------------
    skills = skills_from(current).cache()
    print(f"\n== posting_skills: {skills.count()} rows ==")
    check("no empty skills (drop-expectation preview)",
          skills.filter("skill IS NULL OR skill = ''").count() == 0)
    skills.groupBy("skill_source").count().show(3, False)
    print("top skills:")
    skills.groupBy("skill").count().orderBy(F.desc("count")).show(12, False)

    # ---- salary ---------------------------------------------------------
    fx = spark.createDataFrame(
        [(c, float(r)) for c, r in FX_TO_USD.items()],
        "currency string, usd_rate double")
    base = salary_base_from(postings, fx).cache()
    clean = base.filter("reject_reason IS NULL")
    quarantined = base.filter("reject_reason IS NOT NULL")
    print(f"\n== salary_base: {base.count()} rows with amounts "
          f"({clean.count()} clean / {quarantined.count()} quarantined) ==")
    print("currency x confidence:")
    clean.groupBy("salary_currency", "salary_currency_confidence") \
         .count().orderBy(F.desc("count")).show(12, False)
    print("period confidence:")
    clean.groupBy("salary_period", "salary_period_confidence") \
         .count().orderBy(F.desc("count")).show(10, False)
    print("quarantine reasons:")
    quarantined.groupBy("reject_reason").count().show(5, False)
    check("clean rows all inside plausibility bounds",
          clean.filter(f"salary_mid_annual_usd NOT BETWEEN "
                       f"{SALARY_ANNUAL_USD_MIN} AND {SALARY_ANNUAL_USD_MAX}")
               .count() == 0)
    # Checked against BASE, not clean, deliberately: the committed samples
    # (frozen, so this is deterministic) contain gen_a rows whose "USD" the
    # override corrects to UAH/BYN — after which the honest annualised value
    # is implausible and the bounds gate quarantines them. That interplay is
    # correct behaviour: the override fixes systematic default pollution,
    # and the plausibility gate catches the cases where the source truly
    # meant USD abroad. Overridden rows may therefore live in either half.
    check("gen_a USD-lie override fires somewhere (measured bug!)",
          base.filter("salary_currency_confidence = 'inferred_geo_over_default'")
              .count() > 0)
    lo, hi = (clean.agg(F.min("salary_mid_annual_usd"),
                        F.max("salary_mid_annual_usd")).first())
    print(f"annual USD mid range (clean): {lo:,.0f} .. {hi:,.0f}")

    spark.stop()
    print(f"\n{'ALL CHECKS PASSED' if not failures else 'FAILURES: ' + ', '.join(failures)}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
