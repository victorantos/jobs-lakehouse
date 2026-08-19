#!/usr/bin/env python3
"""Export job postings from the seven career.* board databases to NDJSON.

This is the DATA OWNER's tool — it needs network access to the boards'
SQL Server and credentials. Everyone else uses the committed samples in
data/samples/ and never runs this.

Pipeline position: this script is deliberately dumb extraction ("EL", no "T").
It runs the per-board SELECTs in tools/sql/ (SQL Server's FOR JSON emits one
JSON object per row = NDJSON), validates the output, and cuts the committed
samples. All interpretation — typing, enum decoding, salary/location
normalisation, dedupe — belongs to the Silver layer in Databricks, on purpose:
if a rule changes we replay from Bronze instead of re-exporting.

PII policy (enforced by field selection in tools/sql/*.sql, verified here):
  * no description/requirements text (embeds contact emails)
  * no contact email/phone columns, no application URLs/emails
  * SourceUrl is kept: it is the public posting URL and the strongest
    cross-board duplicate evidence
--check scans every exported value for email-shaped strings as a backstop.

Usage:
  export MSSQL_SERVER="host,port" MSSQL_USER="..." MSSQL_PASSWORD="..."
  python3 tools/export_postings.py --run             # full export
  python3 tools/export_postings.py --run --since 2026-08-01   # incremental
  python3 tools/export_postings.py --check           # validate + PII scan
  python3 tools/export_postings.py --samples         # cut data/samples/
Requires sqlcmd on PATH (macOS: `brew install sqlcmd`).
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SQL_DIR = REPO / "tools" / "sql"
EXPORT_DIR = REPO / "data" / "exports"
SAMPLE_DIR = REPO / "data" / "samples"
SAMPLE_CAP = 500  # rows per board committed to the repo

# board -> (database, sql file, extra sqlcmd -v vars)
BOARDS = {
    "career.coffee":   ("careercoffee-db",  "export_career_coffee.sql",   {}),
    "career.computer": ("CareerComputerDb", "export_career_computer.sql", {}),
    "career.solar":    ("CareerSolarDb",    "export_clone.sql", {"BOARD": "career.solar"}),
    "career.pet":      ("CareerPetDb",      "export_clone.sql", {"BOARD": "career.pet"}),
    "career.delivery": ("CareerDeliveryDb", "export_clone.sql", {"BOARD": "career.delivery"}),
    "career.church":   ("CareerChurchDb",   "export_clone.sql", {"BOARD": "career.church"}),
    "career.dental":   ("CareerDentalDb",   "export_clone.sql", {"BOARD": "career.dental"}),
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def run_exports(since: str) -> None:
    server = os.environ.get("MSSQL_SERVER")
    user = os.environ.get("MSSQL_USER")
    password = os.environ.get("MSSQL_PASSWORD")
    if not all([server, user, password]):
        sys.exit("Set MSSQL_SERVER, MSSQL_USER, MSSQL_PASSWORD (see docstring).")

    stamp = date.today().isoformat()
    for board, (db, sql_file, extra) in BOARDS.items():
        out_dir = EXPORT_DIR / board
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"postings_{stamp}.ndjson"
        cmd = [
            "sqlcmd", "-S", server, "-U", user, "-P", password, "-C",
            "-d", db, "-h", "-1", "-y", "0", "-W",
            "-v", f"SINCE={since}",
            *[x for k, v in extra.items() for x in ("-v", f"{k}={v}")],
            "-i", str(SQL_DIR / sql_file), "-o", str(out),
        ]
        print(f"exporting {board} ({db}) -> {out.relative_to(REPO)}")
        subprocess.run(cmd, check=True)
    normalize_and_check()


def _iter_export_files():
    for board in BOARDS:
        for f in sorted((EXPORT_DIR / board).glob("*.ndjson")):
            yield board, f


def normalize_and_check() -> None:
    """Normalise line endings, validate every line parses as JSON, and REDACT
    email-shaped strings in every field except SourceUrl (postings sometimes
    embed a contact email in the title). Rewrites files in place; a JSON parse
    failure is fatal — it means a line-breaking character escaped the SQL-side
    scrubbing (see the U+2028 note in tools/sql/export_career_coffee.sql).

    NB: split on '\\n' explicitly, NOT str.splitlines() — splitlines() also
    splits on U+2028/U+2029 and would report false parse failures for exactly
    the character class we scrub."""
    bad = 0
    manifest = {}
    for board, f in _iter_export_files():
        rows, redactions = [], 0
        for i, line in enumerate(f.read_text(encoding="utf-8").split("\n"), 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                print(f"  BAD JSON {f.name}:{i}: {line[:120]}")
                bad += 1
                continue
            for k, v in obj.items():
                if k != "SourceUrl" and isinstance(v, str) and EMAIL_RE.search(v):
                    obj[k] = EMAIL_RE.sub("[email-redacted]", v)
                    redactions += 1
            rows.append(json.dumps(obj, ensure_ascii=False))
        f.write_text("\n".join(rows) + "\n", encoding="utf-8")
        manifest.setdefault(board, []).append(
            {"file": f.name, "rows": len(rows), "emails_redacted": redactions})
        print(f"{board}: {f.name} rows={len(rows)} emails_redacted={redactions}")
    (EXPORT_DIR / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if bad:
        sys.exit(f"{bad} unparseable line(s) — fix the export before committing samples.")


# Cross-board pairs to force into the samples so the silver dedupe is
# demonstrable at sample scale: a plain stride almost never catches BOTH
# halves of a duplicate pair (500 of 40k rows each side).
DUP_PAIRS = [("career.computer", "career.solar"), ("career.coffee", "career.computer")]
DUP_PAIRS_PER_COMBO = 25


def _rows_by_url(lines):
    out = {}
    for ln in lines:
        url = json.loads(ln).get("SourceUrl")
        if url and len(url) > 10:
            out.setdefault(url.rstrip("/").lower(), ln)
    return out


def cut_samples() -> None:
    """Deterministic per-board sample: an every-Nth stride (rows are ordered
    by Id ≈ chronological, so the stride gives temporal spread) PLUS a fixed
    quota of cross-board duplicate pairs (same SourceUrl on two boards) so
    the dedupe machinery has something real to find even at sample scale."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    full = {}
    for board in BOARDS:
        files = sorted((EXPORT_DIR / board).glob("*.ndjson"))
        if files:
            full[board] = [ln for ln in
                           files[-1].read_text(encoding="utf-8").splitlines() if ln]

    # pick duplicate pairs from the full exports (sorted for determinism)
    forced = {board: [] for board in full}
    for a, b in DUP_PAIRS:
        if a not in full or b not in full:
            continue
        by_url_a, by_url_b = _rows_by_url(full[a]), _rows_by_url(full[b])
        shared = sorted(set(by_url_a) & set(by_url_b))[:DUP_PAIRS_PER_COMBO]
        forced[a] += [by_url_a[u] for u in shared]
        forced[b] += [by_url_b[u] for u in shared]
        print(f"forcing {len(shared)} duplicate pairs {a} <-> {b} into samples")

    for board, lines in full.items():
        stride = max(1, len(lines) // SAMPLE_CAP)
        sample = lines[::stride][:SAMPLE_CAP]
        seen = {json.loads(ln)["Id"] for ln in sample}
        extra = [ln for ln in forced[board]
                 if json.loads(ln)["Id"] not in seen]
        sample += extra
        out = SAMPLE_DIR / f"{board}.ndjson"
        out.write_text("\n".join(sample) + "\n", encoding="utf-8")
        print(f"{board}: sampled {len(sample)}/{len(lines)} "
              f"({len(extra)} forced dup rows) -> {out.relative_to(REPO)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", action="store_true", help="run the SQL exports")
    ap.add_argument("--since", default="1900-01-01",
                    help="only rows created/updated on or after this date")
    ap.add_argument("--check", action="store_true", help="validate + PII scan")
    ap.add_argument("--samples", action="store_true", help="cut committed samples")
    args = ap.parse_args()
    if args.run:
        run_exports(args.since)
    if args.check and not args.run:  # --run already checks
        normalize_and_check()
    if args.samples:
        cut_samples()
    if not (args.run or args.check or args.samples):
        ap.print_help()
