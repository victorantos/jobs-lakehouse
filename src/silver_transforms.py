"""The silver layer's transforms as pure DataFrame -> DataFrame functions.

Why this file exists separately from pipelines/silver.py: the pipeline file
is *declarative* — table names, comments, expectations, CDC wiring. The
actual logic lives here as plain functions with no pipeline imports, so the
EXACT code the pipeline runs can also run on a laptop against the committed
samples (tools/smoke_test_silver_logic.py) — tested and deployed logic
cannot drift because they are the same object. The .NET parallel: keep
business logic out of the controllers.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import (
    PERIOD_INFER_HOURLY_BELOW_USD,
    PERIOD_INFER_MONTHLY_BELOW_USD,
    SALARY_ANNUAL_USD_MAX,
    SALARY_ANNUAL_USD_MIN,
)
from src.conform import (
    country_default_currency,
    country_iso2,
    employment_type_canonical,
    fx_usd_rate,
    location_parts,
    norm_text,
    period_annual_multiplier,
    seniority_canonical,
    seniority_source,
    status_canonical,
)


# The bronze columns this transform consumes, with the type each is expected
# to carry. Bronze's schema is INFERRED from whatever files actually landed —
# a column that happens to be absent from every ingested file (RemoteType is
# non-null on only 1.4% of computer rows, so a small sample can easily miss
# it) simply doesn't exist, and referencing it would crash the pipeline.
# ensure_columns() materialises missing ones as typed NULLs: the transform
# declares its input contract instead of trusting inference.
BRONZE_INPUT_CONTRACT = {
    "board": "string", "schema_gen": "string", "exported_at": "string",
    "Id": "string", "Title": "string", "CompanyName": "string",
    "JobType": "string", "EmploymentType": "string",
    "ExperienceLevel": "string", "Status": "string",
    "Location": "string", "City": "string", "State": "string",
    "Country": "string", "IsRemote": "string", "IsHybrid": "string",
    "RemoteType": "string", "SalaryMin": "string", "SalaryMax": "string",
    "SalaryCurrency": "string", "SalaryPeriod": "string",
    "RequiredSkills": "string", "Tags": "array<string>",
    "SourceUrl": "string", "SourceHash": "string",
    "PostedAt": "string", "CreatedAt": "string", "UpdatedAt": "string",
    "LastSeenAt": "string", "ExpiresAt": "string",
}


def ensure_columns(df: DataFrame, contract: dict) -> DataFrame:
    """Add any contract column the DataFrame lacks as a typed NULL."""
    present = set(df.columns)
    for name, dtype in contract.items():
        if name not in present:
            df = df.withColumn(name, F.lit(None).cast(dtype))
    return df


def typed_postings(bronze: DataFrame) -> DataFrame:
    """Row-local conformance: bronze's three raw dialects -> one typed,
    canonical shape. Everything here must be computable per row (no windows,
    no joins) so it stays legal under streaming."""
    bronze = ensure_columns(bronze, BRONZE_INPUT_CONTRACT)

    # try_cast, not cast: serverless runs ANSI SQL mode, where CAST of a
    # malformed string is a runtime ERROR. try_cast returns NULL instead —
    # NULLs are visible to expectations; errors just kill the run.
    def ts(name):
        return F.col(name).try_cast("timestamp")

    def money(name):
        c = F.col(name).try_cast("decimal(18,2)")
        return F.when(c > 0, c)  # 0 and negatives are "not published"

    loc = location_parts(F.col("Location"))
    country = F.coalesce(country_iso2(F.col("Country")), loc["country"])
    city = F.coalesce(F.nullif(F.trim(F.col("City")), F.lit("")), loc["city"])
    region = F.coalesce(F.nullif(F.trim(F.col("State")), F.lit("")), loc["region"])

    sal_min = money("SalaryMin")
    sal_max = money("SalaryMax")
    has_amounts = sal_min.isNotNull() | sal_max.isNotNull()
    currency_src = F.nullif(F.upper(F.trim(F.col("SalaryCurrency"))), F.lit(""))
    geo_currency = country_default_currency(country)

    # Trust rules for the measured "lying USD default" (gen_a stamps
    # USD/yearly on every row): a currency is only believed when amounts
    # exist, and gen_a's USD is overridden by unambiguous geography.
    gen_a_usd_lie = ((F.col("schema_gen") == "gen_a")
                     & (currency_src == "USD")
                     & geo_currency.isNotNull() & (geo_currency != "USD"))
    currency_eff = (F.when(~has_amounts, F.lit(None))
                     .when(gen_a_usd_lie, geo_currency)
                     .otherwise(F.coalesce(currency_src, geo_currency)))
    currency_conf = (F.when(~has_amounts, F.lit(None))
                      .when(gen_a_usd_lie, F.lit("inferred_geo_over_default"))
                      .when(currency_src.isNotNull(), F.lit("published"))
                      .when(geo_currency.isNotNull(), F.lit("inferred_geo")))

    # Period: use the published one; else infer from magnitude (in USD) and
    # SAY SO via the confidence column. 609 computer rows have EUR amounts
    # with no period — inference beats discarding, but only if it's flagged.
    period_src = F.when(
        F.lower(F.trim(F.col("SalaryPeriod"))).isin(
            "yearly", "monthly", "weekly", "daily", "hourly"),
        F.lower(F.trim(F.col("SalaryPeriod"))))
    mid_src = F.coalesce((sal_min + sal_max) / 2, sal_min, sal_max)
    mid_usd_as_is = mid_src * fx_usd_rate(currency_eff)
    period_eff = F.coalesce(
        period_src,
        F.when(mid_usd_as_is < PERIOD_INFER_HOURLY_BELOW_USD, "hourly")
         .when(mid_usd_as_is < PERIOD_INFER_MONTHLY_BELOW_USD, "monthly")
         .when(mid_usd_as_is.isNotNull(), "yearly"))
    period_conf = (F.when(period_src.isNotNull(), F.lit("published"))
                    .when(period_eff.isNotNull(), F.lit("inferred_magnitude")))

    # Cross-board duplicate key: exact SourceUrl when the source gave us one
    # (strongest evidence — 303 computer<->solar and 198 coffee<->computer
    # exact matches measured), else a normalised company+title+geo
    # fingerprint. Per-board ids are useless across boards by construction.
    url_norm = F.nullif(
        F.regexp_replace(F.lower(F.trim(F.col("SourceUrl"))), r"/+$", ""),
        F.lit(""))
    fingerprint = F.sha2(F.concat_ws(
        "||",
        F.coalesce(norm_text(F.col("CompanyName")), F.lit("")),
        F.coalesce(norm_text(F.col("Title")), F.lit("")),
        F.coalesce(country, F.lit("")),
        F.coalesce(norm_text(city), F.lit(""))), 256)

    return bronze.select(
        F.concat_ws(":", F.col("board"), F.col("Id")).alias("posting_key"),
        F.col("board"),
        F.col("Id").try_cast("bigint").alias("source_id"),
        F.col("schema_gen"),
        F.col("Title").alias("title"),
        F.col("CompanyName").alias("company_name"),
        employment_type_canonical(F.col("JobType"), F.col("EmploymentType"))
            .alias("employment_type"),
        F.coalesce(F.col("JobType"), F.col("EmploymentType"))
            .alias("employment_type_raw"),
        seniority_canonical(F.col("ExperienceLevel"), F.col("Title"))
            .alias("seniority"),
        seniority_source(F.col("ExperienceLevel"), F.col("Title"))
            .alias("seniority_source"),
        status_canonical(F.col("Status")).alias("status"),
        # coalesce each leg before OR-ing: in three-valued logic
        # false OR NULL = NULL, and RemoteType is null on most rows.
        (F.coalesce(F.col("IsRemote").try_cast("boolean"), F.lit(False))
         | loc["is_remote_text"]
         | F.coalesce(F.col("RemoteType") == "FullyRemote", F.lit(False))
         ).alias("is_remote"),
        F.coalesce(F.col("IsHybrid").try_cast("boolean"), F.lit(False))
            .alias("is_hybrid"),
        city.alias("city"),
        region.alias("region"),
        country.alias("country"),
        F.col("Location").alias("location_raw"),
        loc["quality"].alias("location_quality"),
        sal_min.alias("salary_min_src"),
        sal_max.alias("salary_max_src"),
        currency_src.alias("salary_currency_src"),
        currency_eff.alias("salary_currency"),
        currency_conf.alias("salary_currency_confidence"),
        period_src.alias("salary_period_src"),
        period_eff.alias("salary_period"),
        period_conf.alias("salary_period_confidence"),
        F.col("RequiredSkills").alias("skills_json"),
        F.col("Tags").alias("tags"),
        F.col("SourceUrl").alias("source_url"),
        F.col("SourceHash").alias("source_hash"),
        F.coalesce(F.concat(F.lit("url:"), url_norm),
                   F.concat(F.lit("fp:"), fingerprint)).alias("dup_key"),
        F.coalesce(ts("PostedAt"), ts("CreatedAt")).alias("posted_at"),
        ts("UpdatedAt").alias("updated_at"),
        ts("LastSeenAt").alias("last_seen_at"),
        ts("ExpiresAt").alias("expires_at"),
        F.col("exported_at").try_cast("timestamp").alias("exported_at"),
    )


# The consumer-facing column set, in DDL order — shared by the pipeline's
# schema declaration and the enrich select below.
POSTINGS_COLUMNS = [
    "posting_key", "board", "source_id", "schema_gen", "title",
    "company_name", "employment_type", "employment_type_raw",
    "seniority", "seniority_source", "status", "is_remote",
    "is_hybrid", "city", "region", "country", "location_raw",
    "location_quality", "salary_min_src", "salary_max_src",
    "salary_currency", "salary_currency_confidence", "salary_period",
    "salary_period_confidence", "source_url", "source_hash",
    "dup_key", "dup_group_size", "is_canonical",
    "is_cross_board_duplicate", "posted_at", "updated_at",
    "last_seen_at", "expires_at", "exported_at",
]


def enrich_postings(current: DataFrame) -> DataFrame:
    """Cross-row work over the deduplicated current set: duplicate groups by
    dup_key, canonical flags, group sizes. Needs to see ALL rows, which is
    why it's a materialized view and not a streaming table."""
    group = Window.partitionBy("dup_key")
    rank = Window.partitionBy("dup_key").orderBy(
        F.col("posted_at").asc_nulls_last(), F.col("board"), F.col("source_id"))
    return (
        current
        .withColumn("dup_group_size", F.count("*").over(group))
        .withColumn("is_canonical", F.row_number().over(rank) == 1)
        .withColumn("is_cross_board_duplicate",
                    F.size(F.collect_set("board").over(group)) > 1)
        .select(*POSTINGS_COLUMNS)
    )


def skills_from(current: DataFrame) -> DataFrame:
    """Three skill encodings -> (posting_key, skill) rows: gen_a's JSON
    array serialised INTO A STRING (double-parsed), gen_b/c tag arrays.
    explode (not explode_outer): postings without skills contribute nothing
    rather than a null row."""
    gen_a = (current.select(
                 "posting_key", "board",
                 F.explode(F.from_json(F.col("skills_json"), "array<string>"))
                  .alias("skill_raw"))
             .withColumn("skill_source", F.lit("required_skills_json")))
    tagged = (current.select(
                  "posting_key", "board",
                  F.explode(F.col("tags")).alias("skill_raw"))
              .withColumn("skill_source", F.lit("tags")))
    return (gen_a.unionByName(tagged)
            .withColumn("skill", norm_text(F.col("skill_raw")))
            .select("posting_key", "board", "skill", "skill_raw", "skill_source")
            .dropDuplicates(["posting_key", "skill"]))


def salary_base_from(postings: DataFrame, fx: DataFrame) -> DataFrame:
    """Postings with published amounts, joined to the FX dimension and
    annualised to USD, with a reject_reason column (NULL = clean). The
    pipeline consumes this twice: salary_facts keeps the clean rows (via
    drop expectations, so the counts hit the event log), salary_quarantine
    materialises the rejects."""
    mult = period_annual_multiplier(F.col("salary_period"))
    mid_src = F.coalesce(
        (F.col("salary_min_src") + F.col("salary_max_src")) / 2,
        F.col("salary_min_src"), F.col("salary_max_src"))

    def annual(c):
        return (c * mult * F.col("usd_rate")).cast("double")

    base = (
        postings
        .filter(F.col("salary_min_src").isNotNull()
                | F.col("salary_max_src").isNotNull())
        .join(fx, postings.salary_currency == fx.currency, "left")
        .select(
            "posting_key", "board", "country", "seniority",
            "employment_type", "is_canonical",
            "salary_currency", "salary_currency_confidence",
            "salary_period", "salary_period_confidence",
            "salary_min_src", "salary_max_src", "usd_rate",
            annual(F.col("salary_min_src")).alias("salary_min_annual_usd"),
            annual(F.col("salary_max_src")).alias("salary_max_annual_usd"),
            annual(mid_src).alias("salary_mid_annual_usd"),
            "posted_at",
        )
    )
    reason = (
        F.when(F.col("usd_rate").isNull(), "unknown_currency")
         .when(F.col("salary_period").isNull(), "unknown_period")
         .when(~F.col("salary_mid_annual_usd").between(
             SALARY_ANNUAL_USD_MIN, SALARY_ANNUAL_USD_MAX),
             "annual_out_of_bounds")
    )
    return base.withColumn("reject_reason", reason)
