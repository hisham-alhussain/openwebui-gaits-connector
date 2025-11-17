#!/usr/bin/env python3
"""
transform_excel.py
-----------------
Read the raw GAITS export, map/clean columns, and emit a canonical CSV/Excel
matching the minimal data model required by the Open‑WebUI agent.

Usage:
    python scripts/transform_excel.py <input_path> [--out <output_path>]

Dependencies (add to requirements.txt if not present):
    pandas, openpyxl, pyyaml, python-dateutil
"""

import argparse
import json
import pathlib
import sys
from datetime import datetime
from typing import List, Dict

import pandas as pd
import yaml
from dateutil import parser as dt_parser


# ----------------------------------------------------------------------
# 1️⃣ Load mapping configuration (you can edit column_map.yaml later)
# ----------------------------------------------------------------------
DEFAULT_MAP = {
    "ProjectID": "No.",
    "Name": "Title",
    "Status": "Status",
    "Owner": "Project Manager",          # fallback will be Project Proponent if empty
    "Budget": None,                      # not present – will be NaN -> None
    "LastUpdated": "Latest Check-in When",
    # Any columns you want to preserve verbatim go under `extra`
    "extra": [
        "Progress",
        "Is external project?",
        "Has implementation plan?",
        "Latest Check-in By",
        "Latest Check-in 145"
    ],
}

def load_config(path: pathlib.Path) -> Dict:
    if not path.exists():
        return DEFAULT_MAP
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg


# ----------------------------------------------------------------------
# 2️⃣ Helper functions for cleaning / conversion
# ----------------------------------------------------------------------
def normalize_status(raw: str) -> str:
    """
    Map GAITS free‑text status to our enum.
    Adjust the mapping table as your business rules evolve.
    """
    mapping = {
        "Planning": "Planning",
        "In Progress": "Construction",
        "InDesign": "InDesign",
        "Construction": "Construction",
        "Completed": "Completed",
        "On Hold": "OnHold",
        "OnHold": "OnHold",
    }
    raw_clean = raw.strip()
    return mapping.get(raw_clean, "Planning")   # fallback to Planning


def parse_datetime(cell) -> str:
    """
    Accepts many date‑time shapes (e.g. "Oct 14, 2025 10:4 AM")
    Returns ISO‑8601 string with a trailing "Z" (UTC).
    """
    if pd.isna(cell):
        return None
    try:
        dt = dt_parser.parse(str(cell), fuzzy=True)
        # Force UTC – GAITS timestamps are already UTC in our assumptions
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception as exc:
        raise ValueError(f"Could not parse date '{cell}': {exc}") from exc


def collapse_owners(cell) -> str:
    """
    GAITS may store multiple owners separated by commas, semicolons,
    or the string " and ".
    Normalise to a single comma‑separated string.
    """
    if pd.isna(cell):
        return ""
    # split on common delimiters
    parts = [p.strip() for p in
             pd.Series(str(cell)).str.replace(r"\s+and\s+", ",", regex=True)
             .str.replace(r"[;|]", ",", regex=True).iloc[0].split(",")]
    # remove empty strings and deduplicate while preserving order
    seen = set()
    cleaned = []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            cleaned.append(p)
    return ", ".join(cleaned)


# ----------------------------------------------------------------------
# 3️⃣ Main transformation logic
# ----------------------------------------------------------------------
def transform(
    src_path: pathlib.Path,
    dst_path: pathlib.Path,
    cfg: Dict,
) -> pathlib.Path:
    # --------------------------------------------------------------
    # 3.1 Load the raw sheet (let pandas infer the header row)
    # --------------------------------------------------------------
    df_raw = pd.read_excel(src_path, engine="openpyxl", dtype=str)

    # --------------------------------------------------------------
    # 3.2 Build the canonical dataframe with the minimal columns
    # --------------------------------------------------------------
    rows: List[Dict] = []

    for idx, raw_row in df_raw.iterrows():
        # ---- ProjectID -------------------------------------------------
        pid_raw = raw_row.get(cfg["ProjectID"])
        if pd.isna(pid_raw):
            # Skip rows without an ID – they are likely footers or empty lines
            continue
        project_id = str(pid_raw).strip()

        # ---- Name ------------------------------------------------------
        name = str(raw_row.get(cfg["Name"], "")).strip()

        # ---- Status ----------------------------------------------------
        status_raw = raw_row.get(cfg["Status"], "")
        status = normalize_status(status_raw)

        # ---- Owner -----------------------------------------------------
        owner_raw = raw_row.get(cfg["Owner"])
        # Fallback to Project Proponent if Manager is empty
        if pd.isna(owner_raw) or not owner_raw.strip():
            owner_raw = raw_row.get("Project Proponent")
        owner = collapse_owners(owner_raw)

        # ---- Budget ----------------------------------------------------
        # Not in the sheet – keep as None (will become NULL in DB)
        budget = None

        # ---- LastUpdated ------------------------------------------------
        last_updated_raw = raw_row.get(cfg["LastUpdated"])
        last_updated = parse_datetime(last_updated_raw)

        # ---- Extra JSON (optional) ------------------------------------
        extra_dict = {}
        for col in cfg.get("extra", []):
            extra_dict[col] = raw_row.get(col)
        extra_json = json.dumps(extra_dict, ensure_ascii=False)

        # ---- Assemble the canonical row --------------------------------
        rows.append({
            "ProjectID": project_id,
            "Name": name,
            "Status": status,
            "Owner": owner,
            "Budget": budget,
            "LastUpdated": last_updated,
            "extra_json": extra_json,
        })

    df_canonical = pd.DataFrame(rows)

    # --------------------------------------------------------------
    # 3.3 Write out the canonical file (CSV is fine for downstream tools)
    # --------------------------------------------------------------
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    df_canonical.to_csv(dst_path, index=False, encoding="utf-8")
    print(f"✅  Transformed {len(df_canonical)} rows → {dst_path}")
    return dst_path


# ----------------------------------------------------------------------
# 4️⃣ CLI entry point
# ----------------------------------------------------------------------
def _parse_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform raw GAITS Excel export into the minimal model."
    )
    parser.add_argument(
        "input_file",
        type=pathlib.Path,
        help="Path to the raw GAITS Excel file (xls/xlsx).",
    )
    parser.add_argument(
        "--out",
        type=pathlib.Path,
        default=None,
        help="Destination CSV/Excel. If omitted, writes to ./data/canonical_{basename}.csv",
    )
    parser.add_argument(
        "--map",
        type=pathlib.Path,
        default=pathlib.Path("column_map.yaml"),
        help="Optional YAML mapping file (defaults to column_map.yaml).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_cli()
    cfg = load_config(args.map)

    out_path = args.out
    if out_path is None:
        out_path = pathlib.Path("data") / f"canonical_{args.input_file.stem}.csv"

    try:
        transform(args.input_file, out_path, cfg)
    except Exception as exc:
        print(f"❌  Transformation failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()