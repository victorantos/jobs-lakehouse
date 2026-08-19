"""Conformance library: turns the seven boards' three raw dialects into one
canonical vocabulary. Used by the Lakeflow pipeline (pipelines/silver.py).

Design choice worth defending: everything here is a *Column expression
builder*, not a UDF. Each function composes `pyspark.sql.functions` into an
expression tree that Spark's optimizer can see, push down and codegen — a
Python UDF would be an opaque per-row callback with serialization overhead
(the difference between an expression the SQL optimizer understands and a
CLR scalar function it must call row by row). The .NET analogy: these are
extension methods that build IQueryable expression trees, not delegates.

Dictionary lookups use F.create_map (a literal MAP expression indexed by
column value); ordered keyword rules compile to a chained CASE WHEN via
functools.reduce. First matching rule wins, so rule ORDER is part of the
spec — reorder only deliberately.
"""

from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as F

from src.coffee_enums import EMPLOYMENT_TYPE, JOB_LISTING_STATUS
from src.config import FX_TO_USD, PERIOD_TO_ANNUAL


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def _lit_map(d: dict) -> Column:
    """A Spark literal MAP from a Python dict; index it with m[col]."""
    items = []
    for k, v in d.items():
        items.append(F.lit(k))
        items.append(F.lit(v))
    return F.create_map(*items)


def norm_text(col: Column) -> Column:
    """lower + trim + collapse internal whitespace — the minimum hygiene
    before any string comparison. NULL-safe (stays NULL)."""
    return F.regexp_replace(F.lower(F.trim(col)), r"\s+", " ")


def _rules(source: Column, rules: list, fallback: Column) -> Column:
    """Compile [(regex, label), ...] into CASE WHEN source RLIKE r THEN label.
    reduce() folds the list right-to-left so the FIRST rule tested is the
    first in the list — order is semantics."""
    expr = fallback
    for pattern, label in reversed(rules):
        expr = F.when(source.rlike(pattern), F.lit(label)).otherwise(expr)
    return expr


# --------------------------------------------------------------------------
# employment type — 120+ observed spellings -> 11 canonical values
# --------------------------------------------------------------------------

# gen_a stores the C# enum ordinal; decode then map to canonical.
_GEN_A_EMPLOYMENT_CANONICAL = {
    "FullTime": "full_time", "PartTime": "part_time", "Contract": "contract",
    "Freelance": "freelance", "Internship": "internship",
    "Seasonal": "seasonal", "Temporary": "temporary",
}

# Ordered keyword rules for the raw gen_b/gen_c strings. Mixed-language on
# purpose: the data contains French (CDI/CDD/stage), German (Ausbildung,
# Zeitarbeit), Dutch (vast, detachering) and Japanese (正社員, パート).
# Known bias, documented: combined values like "Full-time, Part-time" match
# the full_time rule first and classify as full_time.
_EMPLOYMENT_RULES = [
    (r"intern(ship)?\b|\bstage\b|praktik", "internship"),
    (r"apprentice|ausbildung|alternan|apprentissage|\blehr", "apprenticeship"),
    (r"volunt|b[eé]n[eé]volat|unremunerated", "volunteer"),
    (r"season|saison", "seasonal"),
    (r"freelance|self.?employed|independent\b|\bzzp\b|freie mitarbeit|業務委託", "freelance"),
    (r"\btemp\b|temporary|zeitarbeit|per diem|\bprn\b|casual|määräaikainen|intermittent", "temporary"),
    (r"contract|\bcdd\b|fixed.?term|\bftc\b|interim|int[eé]rim|detachering|consultant|contrat|契約社員", "contract"),
    (r"full.?time|fulltime|vollzeit|voltijd|permanent|\bcdi\b|unbefristet|festanstellung|\bvast\b|正社員|indefinite|\bregular\b|employee|フルタイム", "full_time"),
    (r"part.?time|parttime|teilzeit|deeltijd|パート|アルバイト|minijob|werkstudent|working student", "part_time"),
]


def employment_type_canonical(job_type_raw: Column, gen_a_ordinal: Column) -> Column:
    """Canonical employment type from EITHER dialect:
    gen_a → int ordinal via the enum decode maps;
    gen_b/c → keyword rules over the normalized raw string.
    Unmatched non-null strings become 'other' (visible, countable — never
    silently swallowed); null both sides → 'unknown'."""
    # try_cast, not cast: ANSI mode (serverless default) ERRORS on casting
    # a non-numeric string; try_cast yields NULL, which the map lookup and
    # coalesce handle. (First caught by tools/smoke_test_silver_logic.py.)
    gen_a_name = _lit_map(EMPLOYMENT_TYPE)[gen_a_ordinal.try_cast("int")]
    from_gen_a = _lit_map(_GEN_A_EMPLOYMENT_CANONICAL)[gen_a_name]
    from_string = _rules(
        norm_text(job_type_raw),
        _EMPLOYMENT_RULES,
        F.when(job_type_raw.isNotNull() & (F.trim(job_type_raw) != ""), F.lit("other")),
    )
    return F.coalesce(from_gen_a, from_string, F.lit("unknown"))


# --------------------------------------------------------------------------
# seniority — ExperienceLevel strings + title fallback -> 9 canonical values
# --------------------------------------------------------------------------

# Ordered HIGH to LOW so "Senior Manager" classifies as manager and
# "Lead Developer (Senior)" as lead — the most senior signal wins.
_SENIORITY_RULES = [
    (r"executive|\bchief\b|\bvp\b|c.?level|board", "executive"),
    (r"director", "director"),
    (r"manager\b|head of", "manager"),
    (r"\blead\b|principal|\bstaff\b|team lead", "lead"),
    (r"senior|\bsr\b|シニア|ervaren", "senior"),
    (r"medior|intermediate|\bmid\b|mid.?level|middle|professional|experienced|berufserfahren", "mid"),
    (r"entry|junior|\bjr\b|early|graduate|trainee|working student|associate\b|starter", "entry"),
    (r"intern", "internship"),
]


def seniority_canonical(experience_level: Column, title: Column) -> Column:
    """Seniority from the raw ExperienceLevel where present, else parsed out
    of the title (titles like 'Senior Barista', 'Jr. Developer' carry it).
    The two-source fallback is flagged separately (seniority_source)."""
    from_raw = _rules(norm_text(experience_level), _SENIORITY_RULES, F.lit(None))
    from_title = _rules(norm_text(title), _SENIORITY_RULES, F.lit(None))
    return F.coalesce(from_raw, from_title, F.lit("unknown"))


def seniority_source(experience_level: Column, title: Column) -> Column:
    """Provenance flag for the seniority value — 'published' (source field),
    'title_parsed' (regex over the title), or 'none'."""
    from_raw = _rules(norm_text(experience_level), _SENIORITY_RULES, F.lit(None))
    from_title = _rules(norm_text(title), _SENIORITY_RULES, F.lit(None))
    return (F.when(from_raw.isNotNull(), F.lit("published"))
             .when(from_title.isNotNull(), F.lit("title_parsed"))
             .otherwise(F.lit("none")))


# --------------------------------------------------------------------------
# posting status — int ordinals (gen_a) vs strings (gen_b/c)
# --------------------------------------------------------------------------

_STATUS_CANONICAL = {  # canonical vocabulary, both dialects map into it
    "Draft": "draft", "Active": "active", "Paused": "paused",
    "Filled": "filled", "Expired": "expired", "Closed": "closed",
}


def status_canonical(status_raw: Column) -> Column:
    """Bronze keeps status as a string either way ('1' or 'Active').
    Try the gen_a ordinal first, then the case-normalised string.
    try_cast is load-bearing: under ANSI mode, cast('Active' AS INT) is a
    runtime error, not a NULL — this exact line killed the smoke test."""
    gen_a_name = _lit_map(JOB_LISTING_STATUS)[status_raw.try_cast("int")]
    by_name = _lit_map({k.lower(): v for k, v in _STATUS_CANONICAL.items()})[
        F.lower(F.trim(status_raw))]
    return F.coalesce(_lit_map(_STATUS_CANONICAL)[gen_a_name], by_name,
                      F.lit("unknown"))


# --------------------------------------------------------------------------
# geography — country names/codes -> ISO2, free-text location parsing
# --------------------------------------------------------------------------

COUNTRY_NAME_TO_ISO2 = {
    # normalized (lowercase) name -> ISO2. Covers every variant observed in
    # the boards' Country columns plus common local-language names.
    "united states": "US", "usa": "US", "us": "US", "u.s.": "US",
    "united states of america": "US", "united kingdom": "GB", "uk": "GB",
    "great britain": "GB", "england": "GB", "scotland": "GB", "wales": "GB",
    "northern ireland": "GB", "germany": "DE", "deutschland": "DE",
    "france": "FR", "netherlands": "NL", "the netherlands": "NL",
    "nederland": "NL", "switzerland": "CH", "schweiz": "CH", "suisse": "CH",
    "austria": "AT", "österreich": "AT", "belgium": "BE", "belgië": "BE",
    "spain": "ES", "españa": "ES", "italy": "IT", "italia": "IT",
    "ireland": "IE", "portugal": "PT", "poland": "PL", "polska": "PL",
    "czech republic": "CZ", "czechia": "CZ", "denmark": "DK", "sweden": "SE",
    "norway": "NO", "finland": "FI", "greece": "GR", "hungary": "HU",
    "romania": "RO", "bulgaria": "BG", "slovakia": "SK", "slovenia": "SI",
    "croatia": "HR", "lithuania": "LT", "latvia": "LV", "estonia": "EE",
    "luxembourg": "LU", "japan": "JP", "日本": "JP", "china": "CN",
    "south korea": "KR", "korea": "KR", "taiwan": "TW", "hong kong": "HK",
    "singapore": "SG", "malaysia": "MY", "thailand": "TH", "vietnam": "VN",
    "viet nam": "VN", "philippines": "PH", "indonesia": "ID", "india": "IN",
    "united arab emirates": "AE", "uae": "AE", "israel": "IL", "turkey": "TR",
    "türkiye": "TR", "russia": "RU", "ukraine": "UA", "brazil": "BR",
    "brasil": "BR", "mexico": "MX", "méxico": "MX", "peru": "PE",
    "colombia": "CO", "chile": "CL", "argentina": "AR", "canada": "CA",
    "australia": "AU", "new zealand": "NZ", "south africa": "ZA",
    "bahrain": "BH", "armenia": "AM", "belarus": "BY",
}

_US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC",
}

# ISO2 -> default currency, used to (a) correct gen_a's lying USD default
# when the geography clearly disagrees, (b) infer a currency when amounts
# exist without one. Eurozone members share EUR.
COUNTRY_DEFAULT_CURRENCY = {
    "US": "USD", "GB": "GBP", "CH": "CHF", "CA": "CAD", "AU": "AUD",
    "NZ": "NZD", "JP": "JPY", "CN": "CNY", "KR": "KRW", "TW": "TWD",
    "HK": "HKD", "SG": "SGD", "MY": "MYR", "TH": "THB", "VN": "VND",
    "PH": "PHP", "ID": "IDR", "IN": "INR", "AE": "AED", "IL": "ILS",
    "TR": "TRY", "RU": "RUB", "UA": "UAH", "PL": "PLN", "CZ": "CZK",
    "HU": "HUF", "RO": "RON", "SE": "SEK", "DK": "DKK", "NO": "NOK",
    "BR": "BRL", "MX": "MXN", "PE": "PEN", "CO": "COP", "CL": "CLP",
    "ZA": "ZAR", "BH": "BHD", "AM": "AMD", "BY": "BYN",
    **{c: "EUR" for c in ["DE","FR","NL","AT","BE","ES","IT","IE","PT","FI",
                          "GR","LU","SK","SI","HR","LT","LV","EE"]},
}

_REMOTE_PATTERN = (r"remote|home.?office|homeoffice|t[eé]l[eé]travail|"
                   r"fully remote|100% remote|anywhere|work from home|wfh")


def country_iso2(country_raw: Column) -> Column:
    """Country column -> ISO2: pass through 2-letter codes, else look up the
    normalized name. Unknown stays NULL (never guessed)."""
    trimmed = F.trim(country_raw)
    is_code = trimmed.rlike(r"^[A-Za-z]{2}$")
    by_name = _lit_map(COUNTRY_NAME_TO_ISO2)[norm_text(country_raw)]
    return F.when(is_code, F.upper(trimmed)).otherwise(by_name)


def location_parts(location_raw: Column) -> dict:
    """Parse a free-text location ('Edmond, OK' / 'Geldermalsen, Gelderland,
    NL' / 'Remote (EU)') into city / region / country / remote-flag columns.
    This is a comma-splitter with country and US-state dictionaries — a
    deliberate 90% parser, NOT a geocoder; location_quality says which rows
    it actually understood, so nobody mistakes coverage for completeness.
    Returns a dict of named Columns to splat into select()."""
    parts = F.split(F.trim(location_raw), r"\s*,\s*")
    n = F.size(parts)
    first = F.regexp_replace(F.element_at(parts, 1), r"\s*\(.*?\)\s*", "")
    last = F.regexp_replace(F.element_at(parts, -1), r"\s*\(.*?\)\s*", "")
    middle = F.when(n >= 3, F.element_at(parts, 2))

    last_iso = country_iso2(last)
    last_is_us_state = F.upper(F.trim(last)).isin(*_US_STATES)

    country = (F.when(n >= 2,
                      F.coalesce(last_iso,
                                 F.when(last_is_us_state, F.lit("US"))))
                .when(n == 1, country_iso2(first)))
    # a one-part location that resolved to a country ("Deutschland") has no city
    city = (F.when((n == 1) & country.isNotNull(), F.lit(None))
             .otherwise(F.nullif(F.trim(first), F.lit(""))))
    region = F.coalesce(
        F.when(last_is_us_state, F.upper(F.trim(last))),
        F.when(n >= 3, F.trim(middle)),
    )
    is_remote_text = F.coalesce(norm_text(location_raw).rlike(_REMOTE_PATTERN),
                                F.lit(False))
    quality = (F.when(location_raw.isNull() | (F.trim(location_raw) == ""), "empty")
                .when(country.isNotNull() & city.isNotNull(), "parsed_full")
                .when(country.isNotNull() | city.isNotNull(), "parsed_partial")
                .otherwise("unparsed"))
    return {"city": city, "region": region, "country": country,
            "is_remote_text": is_remote_text, "quality": quality}


def country_default_currency(iso2: Column) -> Column:
    """ISO2 -> that country's default currency (NULL when unmapped)."""
    return _lit_map(COUNTRY_DEFAULT_CURRENCY)[iso2]


# --------------------------------------------------------------------------
# salary conversion lookups (pinned tables from src/config.py)
# --------------------------------------------------------------------------

def fx_usd_rate(currency: Column) -> Column:
    """Pinned currency -> USD rate as an expression map (used for the period-
    inference heuristic in postings_typed). The AUTHORITATIVE conversion in
    salary_facts instead JOINS the ref_fx_rates table, so an unknown currency
    fails a referential expectation there rather than silently becoming NULL
    mid-arithmetic. Same numbers, two mechanisms, each doing the job it's
    good at."""
    return _lit_map(FX_TO_USD)[currency]


def period_annual_multiplier(period: Column) -> Column:
    """Canonical pay period -> multiplier to annualise (hourly=2080, ...)."""
    return _lit_map(PERIOD_TO_ANNUAL)[period]
