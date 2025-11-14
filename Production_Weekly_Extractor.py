# Production Weekly Extractor - Combined Tool
# Contains pipeline, compare, and GUI interfaces

# from curses import raw
import sys


# ===== Pipeline =====

#!/usr/bin/env python3
# pw_pipeline.py
# Usage examples:
#   python pw_pipeline.py "C:\PW\Aug14.pdf"
#   python pw_pipeline.py "C:\PW\Inbox" --glob *.pdf
#
# Outputs (next to each input):
#   <name>_cleaned.pdf
#   <name>_cleaned.txt          (full text, deterministic order)
#   pages\<name>_p0001.txt ...  (one file per page for chunking)
#
# Requires: PyMuPDF (fitz). Optional later step uses pandas/rapidfuzz in compare script.

import sys, argparse
from pathlib import Path
import fitz  # PyMuPDF
# Reuse your cleaner:
from remove_bottom_watermark import process_pdf as clean_pdf

# --- Add near your other helpers ---
import datetime as dt
import re

import sys, os

def resource_path(relative_path):
    """ For PyInstaller to find bundled files """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


_MONTHS = {m.lower(): i for i, m in enumerate(
    ["","January","February","March","April","May","June","July","August",
     "September","October","November","December"])}

def _parse_span_flexible(s: str):
    """Parse 'Nov 3 – May 15 2026' / 'November 3 - May 15, 2026' -> (start_date, end_date)."""
    if not s:
        return None
    t = (s or "").strip()
    t = t.replace("—", "-").replace("–", "-").replace(",", "")
    m = re.search(r"([A-Za-z]+)\s+(\d{1,2})\s*-\s*([A-Za-z]+)\s+(\d{1,2})\s+(\d{4})", t)
    if not m:
        return None
    m1, d1, m2, d2, y = m.groups()
    d1, d2, y = int(d1), int(d2), int(y)
    def mnum(tok):
        tok = tok.lower()[:3]
        for full, idx in _MONTHS.items():
            if full.startswith(tok):
                return idx
    a, b = mnum(m1), mnum(m2)
    try:
        end = dt.date(y, b, d2)
        start = dt.date(y, a, d1)

        if end < start:
            # Crosses the New Year (e.g., Nov–Mar)
            start = dt.date(year - 1, a, d1)
        return (start, end)
    except Exception:
        return None
    
def _desc_trailing_year(desc: str) -> int | None:
    m = re.search(r"\(([A-Za-z]+)\s+\d{1,2}\s*[-–]\s*([A-Za-z]+)\s+\d{1,2},\s*(\d{4})\)\s*$", desc or "")
    return int(m.group(3)) if m else None




def _equiv_dates(a: str, b: str) -> bool:
    pa, pb = _parse_span_flexible(a), _parse_span_flexible(b)
    if not pa or not pb:
        # fall back to your existing normalized text compare
        return _equiv(a, b)
    return pa == pb

_TYPE_ALIASES = {
    "television":"series","tv series":"series","tv":"series",
    "feature":"feature film","film":"feature film",
    "feature film":"feature film","series":"series",
}
def _norm_type(s: str) -> str:
    return _TYPE_ALIASES.get((s or "").strip().lower(), (s or "").strip().lower())

def _norm_phone_for_compare(s: str) -> str:
    """Return only the last 10 digits of a phone number for reliable comparison."""
    return re.sub(r"\D", "", s or "")[-10:]  # handles US/Canada 10-digit phones

def _norm_email_for_compare(s: str) -> str:
    """Lowercase canonical email for reliable comparison."""
    m = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", s or "", re.I)
    return m.group(0).lower() if m else ""


def _start_month_from_span(s: str) -> str:
    span = _parse_span_flexible(s)
    return span[0].strftime("%B") if span else (s or "").strip()


def extract_text_structured(pdf_path: Path, out_structured: Path, per_page_dir: Path, per_prod_dir: Path,
                            columns: str = "auto", col_margin: float = 12.0):
    """
    Reads with layout, restores reading order, splits robustly into productions, and writes:
      - per-page dumps      -> per_page_dir / <stem>_p0001.txt, ...
      - whole structured    -> out_structured
      - per-production files-> per_prod_dir / <stem>_prod0001.txt, ...
    """
    # Heuristics tuned for Production Weekly
    TITLE_PATTERNS = [
        # Quoted title + type + optional network + optional date/ê
        re.compile(
        r'^\s*(?:###\s*)?[“"]\s*(?P<title>.+?)[”"]\s+.*?\b('
        r'Series|Feature(?:\s*Film)?|Limited|Mini(?:series)?|Pilot|Short|Docu(?:series|mentary)?)\b'
        r'.*?(?:\d{2}-\d{2}-\d{2}\s*ê?)?$',
        re.I
    ),
        # Unquoted but short headline with a type token and maybe a network slash
        re.compile(r'^[A-Z0-9][^a-z]{1,120}\b(Series|Feature(?:\s*Film)?|Telefilm|Pilot|Short|Docu)\b.*?(?:/\s*\w.*?)?(?:\d{2}-\d{2}-\d{2}\s*ê?)?$', re.I),
    ]
    HEADER_KILL = [
        re.compile(r'^\s*LAST UPDATED:', re.I),
        re.compile(r'^\s*issue\s+\w+', re.I),
        re.compile(r'^\s*#\d+\s+\d{2}/\d{2}\s*$', re.I),
    ]

    def looks_like_title(line: str, max_size: float, is_bold: bool, size_thr: float) -> bool:
        s = line.strip()
        if not s:
            return False
        # Any hard title pattern?
        if any(pat.search(s) for pat in TITLE_PATTERNS):
            return True
        # Uppercase ratio + size/bold fallback
        letters = [c for c in s if c.isalpha()]
        upper_ratio = (sum(c.isupper() for c in letters) / len(letters)) if letters else 0.0
        return (max_size >= size_thr and (is_bold or upper_ratio >= 0.5) and len(s) <= 120)

    doc = fitz.open(pdf_path)
    try:
        per_page_dir.mkdir(parents=True, exist_ok=True)
        per_prod_dir.mkdir(parents=True, exist_ok=True)

        all_prods = []
        cur_prod_lines = []

        for page in doc:
            pnum = page.number + 1
            d = page.get_text("dict")
            width = page.rect.width
            mid = width / 2.0

            # Gather blocks with bbox + text + font metadata
            blocks = []
            for b in d.get("blocks", []):
                if "lines" not in b:
                    continue
                text_lines = []
                sizes, fonts = [], []
                for ln in b["lines"]:
                    line_txt = "".join(sp.get("text", "") for sp in ln.get("spans", []))
                    if line_txt.strip():
                        text_lines.append(line_txt)
                    for sp in ln.get("spans", []):
                        sizes.append(sp.get("size", 0.0))
                        fonts.append(sp.get("font", ""))
                if not text_lines:
                    continue
                text = "\n".join(text_lines).strip()
                x0, y0, x1, y1 = b["bbox"]
                max_size = max(sizes) if sizes else 0.0
                is_bold = any(("Bold" in f) or ("Black" in f) or ("Heavy" in f) for f in fonts)
                blocks.append({
                    "bbox": (x0, y0, x1, y1),
                    "text": text,
                    "max_size": max_size,
                    "is_bold": is_bold,
                })

            # Column indexer
            def col_idx(bl):
                x0, _, x1, _ = bl["bbox"]
                if columns == "1": return 0
                if columns == "2": return 0 if x1 < (mid - col_margin) else 1
                # auto:
                if x1 < (mid - col_margin): return 0
                if x0 > (mid + col_margin): return 1
                return 0

            # Sort reading order
            blocks.sort(key=lambda bl: (col_idx(bl), bl["bbox"][1], bl["bbox"][0]))

            # Size threshold ~85th percentile for this page
            sizes_sorted = sorted(bl["max_size"] for bl in blocks)
            q = max(0, int(0.85 * (len(sizes_sorted) - 1)))
            size_thr = sizes_sorted[q] if sizes_sorted else 0.0

            # Build per-page output while splitting productions line-by-line
            page_dump_lines = []
            for bl in blocks:
                # Skip headers/folio noise
                if any(p.search(bl["text"]) for p in HEADER_KILL):
                    continue

                # Scan each physical line; if a title appears mid-block, split there
                for i, raw in enumerate(bl["text"].splitlines()):
                    line = raw.strip()
                    if not line:
                        page_dump_lines.append("")  # keep light spacing in page dump
                        continue

                    if looks_like_title(line, bl["max_size"], bl["is_bold"], size_thr):
                        # close previous production
                        if cur_prod_lines:
                            # compress extra blank lines
                            text = re.sub(r'\n{3,}', '\n\n', "\n".join(cur_prod_lines).strip())
                            if text:
                                all_prods.append(text)
                            cur_prod_lines = []
                        # start new one
                        cur_prod_lines.append(f"### {line}")
                    else:
                        cur_prod_lines.append(line)

                    page_dump_lines.append(line)

            # Write per-page dump (in case you want the legacy per-page view)
            (per_page_dir / f"{pdf_path.stem}_p{pnum:04d}.txt").write_text(
                "\n\n".join([ln for ln in page_dump_lines if ln is not None]),
                encoding="utf-8"
            )

            # === NEW: force a production break at each page boundary ===
            if cur_prod_lines:
                text = re.sub(r'\n{3,}', '\n\n', "\n".join(cur_prod_lines).strip())
                if text:
                     all_prods.append(text)
                cur_prod_lines = []
# === end NEW ===


        # Flush last production
        if cur_prod_lines:
            text = re.sub(r'\n{3,}', '\n\n', "\n".join(cur_prod_lines).strip())
            if text:
                all_prods.append(text)

        # Whole structured file with clear separators
        out_structured.write_text(
            ("\n\n----- PRODUCTION BREAK -----\n\n").join(all_prods),
            encoding="utf-8"
        )

        # One file per production (easier for GPT ingestion)
        for idx, prod in enumerate(all_prods, 1):
            (per_prod_dir / f"{pdf_path.stem}_prod{idx:04d}.txt").write_text(prod, encoding="utf-8")

    finally:
        doc.close()

def pipeline_build_one(pdf: Path, out_root: Path, skip_clean: bool = True) -> tuple[Path, Path, Path]:
    """
    Clean + extract a single PDF, then run build_cmd on its per-PDF folder.
    Returns (csv_path, baseline_path, per_pdf_out_dir).
    """
    per_pdf_out = out_root / pdf.stem
    per_pdf_out.mkdir(parents=True, exist_ok=True)

    cleaned = per_pdf_out / f"{pdf.stem}_cleaned.pdf"
    structured = per_pdf_out / f"{pdf.stem}_cleaned.structured.txt"
    pages_dir  = per_pdf_out / "pages"
    prods_dir  = per_pdf_out / "productions"

    # Clean
    need_clean = True
    if skip_clean and cleaned.exists():
        try:
            need_clean = cleaned.stat().st_mtime < pdf.stat().st_mtime
        except Exception:
            need_clean = True
    if need_clean:
        clean_pdf(pdf, cleaned, strip=100, units="pixels", dpi=72.0, mode="redact")

    # Extract
    extract_text_structured(cleaned, structured, pages_dir, prods_dir)

    # Build FullSchema in the same per-PDF folder
    csv_path, baseline_path, _filtered = build_cmd(per_pdf_out, per_pdf_out, pdf.stem)
    return Path(csv_path), Path(baseline_path), per_pdf_out


def main(argv=None):
    ap = argparse.ArgumentParser(description="Clean bottom watermark then extract text from PW PDFs.")
    ap.add_argument("input", help="PDF file or directory")
    ap.add_argument("--glob", default="*.pdf", help="Pattern when input is a folder (default: *.pdf)")
    ap.add_argument("--strip", type=float, default=100, help="Bottom strip (default 100)")
    ap.add_argument("--units", choices=["pixels", "points"], default="pixels")
    ap.add_argument("--dpi", type=float, default=72.0)
    ap.add_argument("--mode", choices=["redact", "crop"], default="redact")
    ap.add_argument("--outdir", default=None,
                    help="Root folder where outputs are consolidated. If omitted, defaults to <input_dir>\\pw_output")
    ap.add_argument("--skip-clean", action="store_true",
                    help="If a cleaned PDF already exists and is newer than the source, reuse it")

    args = ap.parse_args(argv or sys.argv[1:])

    src = Path(args.input)
    if src.is_file():
        targets = [src]
    elif src.is_dir():
        targets = sorted(src.glob(args.glob))
    else:
        raise SystemExit(f"Not found: {src}")

    if not targets:
        raise SystemExit("No PDFs matched.")

    for pdf in targets:
        print(f"\n-> Processing: {pdf}")

        # --- Consolidated output roots FIRST ---
        out_root = Path(args.outdir) if args.outdir else (pdf.parent / "pw_output")
        out_dir  = out_root / pdf.stem                  # per-PDF subfolder
        out_dir.mkdir(parents=True, exist_ok=True)

        # All outputs live under out_dir
        cleaned      = out_dir / f"{pdf.stem}_cleaned.pdf"
        out_struct   = out_dir / f"{pdf.stem}_cleaned.structured.txt"
        per_page_dir = out_dir / "pages"
        per_prod_dir = out_dir / "productions"

        # Clean into out_dir (or reuse if permitted)
        need_clean = True
        if args.skip_clean and cleaned.exists():
            try:
                need_clean = cleaned.stat().st_mtime < pdf.stat().st_mtime
            except Exception:
                need_clean = True

        if need_clean:
            clean_pdf(pdf, cleaned, strip=args.strip, units=args.units, dpi=args.dpi, mode=args.mode)

        # Structured extraction (writes per-page + per-production files)
        extract_text_structured(cleaned, out_struct, per_page_dir, per_prod_dir)

        print(f"   Output folder: {out_dir}")
        print(f"     Cleaned PDF : {cleaned}")
        print(f"     Structured  : {out_struct}")
        print(f"     Pages dir   : {per_page_dir}")
        print(f"     Productions : {per_prod_dir}")





# ===== Compare Tool =====

#!/usr/bin/env python3
# pw_compare_tool.py
#
# Deterministic builder + comparator for Production Weekly.
# - build: parse productions -> standardized CSV + baseline/filtered lists
# - compare: two CSVs (+ baselines) -> differences CSV + MasterSchema export
#
# Python 3.13+, local only (no cloud). Requires: geonamescache, pycountry, rapidfuzz

import argparse, csv, re, unicodedata
from pathlib import Path
from datetime import date
from typing import Dict, List, Tuple, Optional

# --- Optional global gazetteer (offline) ---
from functools import lru_cache
import geonamescache, pycountry
from rapidfuzz import fuzz

TITLE_PREFIX = "### "
PROD_BREAK = "----- PRODUCTION BREAK -----"

# ======== Internal Required CSV Schema (EXACT, DO NOT CHANGE) ========
SCHEMA = [
    "Production Name",
    "Format Label",
    "Start Month",
    "Shooting Dates",
    "Actively in Production",
    "If It Was Pushed",
    "Computed Production Length",
    "Description",
    "City",
    "Province/State",
    "Country",
    "Type",
    "Director/Producer",
    "VFX Team",
    "Studio Info",
    "Production Office",
    "Production Phone",
    "Production Email",
    "Production Company",
    "Notes",
    "Category",
    "All Locations",
]

# For comparison CSV: put Category + Notes right after title
COMPARE_SCHEMA = ["Production Name", "Category", "Notes"] + [
    c for c in SCHEMA if c not in ("Production Name", "Category", "Notes")
]

# ======== Master Spreadsheet Schema (export only) ========
MASTER_SCHEMA = [
    "Region Bucket",              
    "Category",             # New / Updated / Removed
    "Production Name",
    "Issue Link",
    "Start Month",
    "Shooting Dates",
    "Actively in Production",
    "Date Pushed Back?",
    "Length (Days)",
    "Description",
    "City",
    "Province/State",
    "Country",        # computed
    "Type",
    "Director/Producer",
    "VFX Notes",
    "IMDb Link",
    "Studio Name",
    "Production Office",
    "Production Phone/Email",
    "Prod. Co",  

]

NA_TOKENS = {"", "n/a", "na", "none", "-", "—"}

def _norm_text(s: str) -> str:
    """Normalizes text for general-purpose, case-insensitive comparison."""
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def _equiv(a: str, b: str) -> bool:
    """Consider blanks and NA placeholders equivalent; compare case-insensitively."""
    sa = _norm_text(a)
    sb = _norm_text(b)
    if sa in NA_TOKENS and sb in NA_TOKENS:
        return True
    return sa == sb

def _norm_key(name: str) -> str:
    """
    Normalizes a production title into a key for matching.
    Strips AKA / W/T aliases in parentheses so things like:
      - Title (AKA: Foo)
      - Title (aka "Foo")
      - Title (w/t "Foo")
      - Title (w.t. Foo)
    all normalize to the same base title.
    """
    if not name:
        return ""

    # Strip parenthetical AKA / W/T clauses, with lots of spelling/format variants
    s = re.sub(
        r'\s*\('
        r'(?:aka|a\.k\.a\.|w[./-]?\s*t)\s*'  # aka / a.k.a. / wt / w/t / w.t etc
        r'[:\-]?\s*'
        r'[^)]*'                             # up to closing paren
        r'\)',
        '',
        name,
        flags=re.I,
    ).strip()

    # Existing cleanup logic
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = (
        s.replace("’", "'")
         .replace("‘", "'")
         .replace("“", '"')
         .replace("”", '"')
         .replace("–", "-")
         .replace("—", "-")
    )
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())



def _extract_alt_titles(text: str) -> str:
    """
    Extract AKA / W/T aliases from a production block.
    Supports quoted (", “ ”, ', ‘ ’) and unquoted forms, plus small punctuation variants.
    Returns a formatted string like "(AKA: Foo, W/T: Bar)" or "" if none.
    """
    if not text:
        return ""

    # Quoted: aka "Foo", w/t “Bar”
    QUOTED = re.compile(
        r'\b(?:aka|w[./-]?\s*t)\b\s*[:=]?\s*'
        r'(?:["“”\'‘’])\s*'
        r'([^"”\'’\r\n]+?)\s*'
        r'(?:["“”\'‘’])',
        re.I
    )

    # Unquoted: aka Foo, aka as Foo, w/t Foo etc.
    UNQUOTED = re.compile(
        r'\b(?:aka|w[./-]?\s*t)\b\s*[:=]?\s*'
        r'(?:as\s+)?'
        r'([^\)\]\|;,\r\n]{2,})',
        re.I
    )

    raw = text
    found: list[tuple[str, str]] = []

    # collect all raw matches
    for m in QUOTED.finditer(raw):
        label = m.group(0).split()[0]   # aka / w/t variant
        alias = m.group(1).strip()
        found.append((label, alias))

    for m in UNQUOTED.finditer(raw):
        label = m.group(0).split()[0]
        alias = m.group(1).strip()
        # Trim trailing cruft like ' |', ')' etc.
        alias = re.sub(r'[\s\)\]\|;,\u2014\u2013-]+$', '', alias).strip()
        found.append((label, alias))

    if not found:
        return ""

    # Normalize labels and dedupe (strip all quote characters)
    pretty = []
    seen = set()
    for label, alias in found:
        tag = "W/T" if label.lower().startswith("w") else "AKA"
        alias_clean = re.sub(r'[“”"\'‘’]', '', alias).strip()
        key = (tag, alias_clean.lower())
        if not alias_clean or key in seen:
            continue
        seen.add(key)
        pretty.append(f"{tag}: {alias_clean}")

    return f"({', '.join(pretty)})" if pretty else ""



def _extract_studio_name(block_lines: List[str]) -> str:
    """
    Finds specific, known studio names from a list of lines using a keyword map
    and returns only their canonical names.
    """
    STUDIO_MAP = {
        "bridge studios": "Bridge Studios", "mammoth studios": "Mammoth Studios",
        "north shore studios": "North Shore Studios", "martini film studios": "Martini Film Studios",
        "vancouver film studios": "Vancouver Film Studios", "pinewood studios": "Pinewood Studios",
        "studio city": "Studio City", "aspect film studios": "Aspect Film Studios",
        "big sky studios": "Big Sky Studios", "santa clarita studios": "Santa Clarita Studios",
        "universal lot studios": "Universal Lot Studios", "culver city studios": "Culver City Studios",
        "origo studios": "Origo Studios"
    }
    found_studios = set()
    for line in block_lines:
        line_lower = line.lower()
        for keyword, canonical_name in STUDIO_MAP.items():
            if keyword in line_lower:
                found_studios.add(canonical_name)
    return " | ".join(sorted(list(found_studios)))


def _extract_all_phones(text: str) -> str:
    """Return only the first phone number in the block."""
    m = RE_PHONE.search(text or "")
    return m.group(0) if m else ""

def _extract_all_emails(text: str) -> str:
    """Return only the first email address in the block."""
    m = RE_EMAIL.search(text or "")
    return m.group(0) if m else ""


def _status_and_location_from_lines(lines: list[str]) -> tuple[str, str]:
    """
    Extract STATUS and LOCATION(S) from block lines.

    Handles:
      - STATUS: March 2, 2026 LOCATION(S): Vancouver, BC   (same line)
      - STATUS: March 2, 2026                             (line 1)
        LOCATION(S): Vancouver, BC                        (line 2)
      - Minor spacing / punctuation variants.
    """
    status_val = ""
    location_val = ""

    for i, ln in enumerate(lines):
        up = ln.upper().strip()

        # --- Case 1: STATUS and LOCATION(S) on the same line ---
        if up.startswith("STATUS") and "LOCATION" in up:
            loc_pos = up.find("LOCATION")           # where LOCATION starts in the uppercased line
            status_part = ln[:loc_pos]             # original casing up to LOCATION
            loc_part = ln[loc_pos:]                # original casing from LOCATION onward

            # STATUS part
            if ":" in status_part:
                status_val = status_part.split(":", 1)[1].strip()
            else:
                status_val = status_part[len("STATUS"):].strip(" :-\t")

            # LOCATION part
            if ":" in loc_part:
                location_val = loc_part.split(":", 1)[1].strip()
            else:
                # e.g. "LOCATION(S) Vancouver, BC"
                after = loc_part.split(None, 1)
                location_val = after[1].strip() if len(after) > 1 else ""

        else:
            # --- Case 2: STATUS on its own line ---
            if up.startswith("STATUS"):
                if ":" in ln:
                    status_val = ln.split(":", 1)[1].strip()
                else:
                    status_val = ln[len("STATUS"):].strip(" :-\t")

            # --- Case 3: LOCATION(S) on its own line (or followed by value) ---
            if up.startswith("LOCATION"):
                if ":" in ln:
                    location_val = ln.split(":", 1)[1].strip()
                else:
                    if i + 1 < len(lines):
                        location_val = lines[i + 1].strip()

        # Once we have both, bail
        if status_val and location_val:
            break

    return status_val, location_val

def _start_month_from_status(status_val: str) -> str:
    """
    Extract a Start Month value from a STATUS line.

    If there is a Month + Year present (e.g. "March 2026" or "March 2, 2026")
    we normalize it to "Month YYYY". Otherwise we fall back to the first token,
    which preserves existing behaviour for values like "Q1 2026" or "Active".
    """
    if not status_val:
        return ""

    txt = (status_val or "").strip()

    # Try to find an explicit Month ... Year pattern anywhere in the status text.
    # This covers:
    #   "March 2026"
    #   "March 2 2026"
    #   "March 2, 2026"
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b"
        r"(?:[^0-9]{0,10}\d{1,2})?"  # optional day
        r"[^0-9]{0,10}(\d{4})",
        txt,
    )
    if m:
        month, year = m.group(1), m.group(2)
        return f"{month} {year}"

    # Fallback: just keep the first token, as before.
    first_tok = txt.split()[0]
    return _normalize_spaces(first_tok)



def _parse_block_to_row(block_text: str) -> Tuple[Dict[str,str], str, bool]:
    lines = [ln.strip() for ln in block_text.splitlines() if ln.strip()]
    first = lines[0] if lines else ""
    
    title = _quoted_title_from_title_line(first)
    alt_title_str = _extract_alt_titles(block_text)
    if alt_title_str:
        title = f"{title} {alt_title_str}"

    prod_type = _type_from_title_line(first)
    format_label = _format_label_from_title_line(first)
    full_text_body = "\n".join(lines[1:]).strip()
    ex_text = (block_text or "").lower()
    is_excluded = any(k in ex_text for k in EXCLUDE_KEYWORDS)

    # Initialize in case we don't find a clean company/address block.
    company_str = ""
    address_str = ""
    office_str = ""

    
    # Search for the main Production Company and its potential address.
    # It is typically the first capitalized block of text after the title.
    for i, line in enumerate(lines):
        # Skip the title line itself.
        if i == 0:
            continue
        
        # Stop searching when we hit the status/location line or major crew credits.
        if line.upper().startswith(("STATUS:", "PRODUCER:", "WRITER:", "DIRECTOR:", "CAST:", "CD:")):
            break
            
        # A company line is typically all-caps or has a high ratio of caps.
        if _upperish(line):
            company_str = _normalize_spaces(line)
            
            # Check if the *next* line is an address.
            if (i + 1) < len(lines) and _addressy(lines[i + 1]):
                address_str = _normalize_spaces(lines[i + 1])
                office_str = f"{company_str} | {address_str}"
            else:
                office_str = company_str # If no address, the office is just the company name.
            
            # We found the first and most important company block, so we stop searching.
            break

    # Independently, search all lines for a known studio from our strict map.
    studio_str = _extract_studio_name(lines)
    
    # Extract other info from the full block for robustness.
    status_val, location_val = _status_and_location_from_lines(lines)
    if not status_val:
        m_status = RE_STATUS.search(block_text)
        if m_status:
            status_val = m_status.group(1).strip()

    if not location_val:
        m_loc = RE_LOCATION.search(block_text)
        if m_loc:
            location_val = m_loc.group(1).strip()
    
    start_month = _start_month_from_status(status_val)
    sd_text, sd, ed = _parse_date_range(block_text)
    active = _active_today(sd, ed)
    length_days = _inclusive_days(sd, ed)
    city, region, country = _city_state_country_from_location(location_val)
    dir_prod = _gather_director_producer(lines)
    vfx = _gather_vfx(lines)
    # --- END NEW ENGINE ---

    row = {k:"" for k in SCHEMA}
    row["Production Name"] = title
    row["Format Label"] = format_label
    row["Start Month"] = start_month
    row["Shooting Dates"] = sd_text
    row["Actively in Production"] = active
    row["If It Was Pushed"] = ""
    row["Computed Production Length"] = length_days
    row["Description"] = full_text_body
    row["City"] = city
    row["Province/State"] = region
    row["Country"] = country
    row["Type"] = prod_type
    row["Director/Producer"] = dir_prod
    row["VFX Team"] = vfx
    row["Studio Info"] = studio_str
    row["Production Office"] = office_str
    row["Production Phone"] = _extract_all_phones(block_text)
    row["Production Email"] = _extract_all_emails(block_text)
    row["Production Company"] = company_str
    row["Notes"] = ""
    row["Category"] = ""
    row["All Locations"] = location_val
    return row, title, is_excluded
# ---------------- MASTER COMPARE ----------------

# Master sheet header aliases (be forgiving)
_MASTER_ALIASES = {
    "production name": "Production Name",
    "title": "Production Name",
    "issue link": "Issue Link",
    "start month": "Start Month",
    "shooting dates": "Shooting Dates",
    "actively in production": "Actively in Production",
    "date pushed back?": "Date Pushed Back?",
    "length (days)": "Length (Days)",
    "description": "Description",
    "city": "City",
    "province/state": "Province/State",
    "state/province": "Province/State",
    "country": "Country",
    "type": "Type",
    "director/producer": "Director/Producer",
    "vfx notes": "VFX Notes",
    "imdb link": "IMDb Link",
    "studio name": "Studio Name",
    "production office": "Production Office",
    "production phone/email": "Production Phone/Email",
    "production company": "Production Company",
    "vfx contact": "VFX Contact",
    "region": "Region",
        # if present, we’ll use it to auto-detect target region
}
_MASTER_ALIASES.update({
    # common variants seen in your sheet
    "production weekly": "Issue Link",
    "act in prod": "Actively in Production",
    "prod. ph# / email": "Production Phone/Email",
    "prod. co.": "Production Company",
    "colour key:": "",  # ignore decorative columns
    "green = reached out or already have it": "",  # ignore
    "yellow = reach out": "",  # ignore
    "red = unsure / contact asap or not at all": "",  # ignore
    "unnamed: 0": "", "unnamed: 1": "", "unnamed: 2": "",  # ignore empty columns if they slip in
})


def _canon_header(name: str) -> str:
    return _MASTER_ALIASES.get((name or "").strip().lower(), name)

def _read_master_rows(path: Path) -> list[dict]:
    """
    Read a 'decorated' master CSV where the true column names may appear in a later row.
    We scan the top of the file to find a row that contains 'Production Name' and promote it as header.
    """
    import csv

    # Read all rows first (UTF-8 with BOM safe)
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))

    if not rows:
        return []

    # Find header row index: first row that has a cell equal to 'Production Name' (case-insensitive)
    header_idx = -1
    for i in range(min(20, len(rows))):
        row_lower = [ (c or "").strip().lower() for c in rows[i] ]
        if "production name".lower() in row_lower:
            header_idx = i
            break

    # Fallback: just use the file's first row
    if header_idx == -1:
        header_idx = 0

    headers_raw = rows[header_idx]
    # Canonicalize headers with alias map
    headers = [_canon_header(h) for h in headers_raw]

    out: list[dict] = []
    for r in rows[header_idx+1:]:
        if not any((c or "").strip() for c in r):
            continue  # skip empty lines
        rec = {}
        for i, h in enumerate(headers):
            if not h:  # skip empty header cells
                continue
            val = r[i] if i < len(r) else ""
            rec[h] = val
        # Keep only rows that actually have a production name
        if (rec.get("Production Name","") or "").strip():
            out.append(rec)
    return out


def _region_from_master(rows: list[dict]) -> str:
    # Try to infer a single region label from the master file (if it has a "Region" column)
    vals = { (r.get("Region","") or "").strip() for r in rows }
    vals = {v for v in vals if v}
    return vals.pop() if len(vals) == 1 else ""

# fields we’ll compare between master and weekly (use the names you keep on master)
_FIELDS_TO_COMPARE_AGAINST_MASTER = [
    "Production Name",
    "Shooting Dates",
    "Start Month",
    "City",
    "Province/State",
    "Country",
    "Type",
    "Director/Producer",
    "Production Company",
    # deliberately NOT comparing: "Issue Link", "Description"
]

def resolve_run_folder(run_dir: Path):
    """Given a run folder, return (csv, baseline, label)."""
    csv_files = list(run_dir.glob("*_FullSchema.csv"))
    base_files = list(run_dir.glob("*_baseline_titles.txt"))
    if not csv_files or not base_files:
        raise FileNotFoundError(f"Run folder {run_dir} missing expected files")
    csv_path = csv_files[0]
    baseline_path = base_files[0]
    # Label is just the base stem without suffix
    label = csv_path.stem.replace("_FullSchema", "")
    return csv_path, baseline_path, label


def _changed_vs_master(master: dict, weekly: dict) -> list[str]:
    diffs = []
    for f in _FIELDS_TO_COMPARE_AGAINST_MASTER:
        ov = (master.get(f, "") or "")
        nv = (weekly.get(f, "") or "")

        # --- NEW: treat "weekly blank, master filled" as a parsing miss, not a change ---
        if _norm_text(nv) in NA_TOKENS and _norm_text(ov) not in NA_TOKENS:
            # Skip flagging this field as changed
            continue

        if f == "Production Name":
            if _norm_key(ov) != _norm_key(nv):
                diffs.append(f)

        elif f == "Shooting Dates":
            if not _equiv_dates(ov, nv):
                diffs.append(f)

        elif f == "Type":
            if _norm_type(ov) != _norm_type(nv):
                diffs.append(f)

        elif f == "Start Month":
            if _start_month_from_span(master.get("Shooting Dates", "")) \
               != _start_month_from_span(weekly.get("Shooting Dates", "")):
                diffs.append(f)

        elif f == "Production Phone":
            if _norm_phone_for_compare(ov) != _norm_phone_for_compare(nv):
                diffs.append(f)

        elif f == "Production Email":
            if _norm_email_for_compare(ov) != _norm_email_for_compare(nv):
                diffs.append(f)

        else:
            if not _equiv(ov, nv):
                diffs.append(f)

    return diffs



def _weekly_to_master_projection(weekly_row: dict, region_label: str, issue_link: str) -> dict:
    """Shape a weekly row to your MASTER_SCHEMA columns."""
    proj = to_master_row(weekly_row, issue_link=issue_link)  # uses your existing mapping & region_bucket
    # if you want to force region to the target label (sheet-level), uncomment:
    # proj["Region"] = region_label or proj.get("Region","")
    return proj

REGION_FILE_MAP = {
    "United States": "United States",
    "Quebec": "Quebec",
    "West Coast Canada": "West Coast CA",
    "East Coast Canada": "East Coast CA",
    "Ireland/Hungary": "Ireland_Hungary",
    "Australia/New Zealand": "Australia_NewZealand",
    "Europe/Other": "Europe_Other",
    "Other": "Other",
}

def master_compare_cmd(
    master_dir: Path,
    weekly_csv: Path, weekly_baseline: Path, weekly_label: str,
    outdir: Path, latest_date_for_filename: str,
    region: str,
    summary_acc: dict | None = None,
):
    # Make sure output dir exists
    outdir.mkdir(parents=True, exist_ok=True)

    # --- Resolve master CSV robustly ---
    md = Path(master_dir)
    if md.is_file() and md.suffix.lower() == ".csv":
        master_csv = md
    else:
        # treat as directory: pick a csv whose name contains the region key
        region_key = REGION_FILE_MAP.get(region, region)
        target = (region_key or "").lower().replace(" ", "").replace("_", "").replace("-", "")
        candidates = list(md.glob("*.csv"))
        if not candidates:
            raise FileNotFoundError(f"No CSV files found in {md}")
        master_csv = None
        for f in candidates:
            stem = f.stem.lower().replace(" ", "").replace("_", "").replace("-", "")
            if target and target in stem:
                master_csv = f
                break
        if master_csv is None:
            # fallback: first CSV
            master_csv = candidates[0]

    print(f"[master-compare] Using master file: {master_csv}")
    master_rows = _read_master_rows(master_csv)

    # Load weekly rows + baseline index
    weekly_rows = _read_csv_rows(weekly_csv)
    base_titles = _read_titles_list(weekly_baseline)
    baseline_index = { _norm_key(t): i+1 for i, t in enumerate(base_titles) }

    # Try to auto-detect region from master if not passed
    if not region:
        region = _region_from_master(master_rows)

    # Build maps by normalized title
    m_map = {}
    for r in master_rows:
        k = _norm_key(r.get("Production Name",""))
        if k and k not in m_map:
            m_map[k] = r

    # Optionally filter weekly to the same region bucket as the master
    w_kept = []
    for r in weekly_rows:
        if region:
            bucket = region_bucket(r.get("City",""), r.get("Province/State",""), r.get("Country",""))
            if bucket != region:
                continue
        w_kept.append(r)

    w_map = {}
    w_order = []
    for r in w_kept:
        k = _norm_key(r.get("Production Name",""))
        if k and k not in w_map:
            w_map[k] = r
            w_order.append(k)

    # --- Compare ---
    diff_rows_master: list[dict] = []

    for k in w_order:
        weekly_record = w_map[k]
        diffs: list[str] = []

        if k in m_map:
            # --- UPDATED vs Master ---
            master_record = m_map[k]
            diffs = _changed_vs_master(master_record, weekly_record)

            if diffs:
                # Detect “pushed back”
                ms = _start_date_from_shooting_dates(master_record.get("Shooting Dates",""))
                ws = _start_date_from_shooting_dates(weekly_record.get("Shooting Dates",""))

                # If we can't parse concrete shooting dates, fall back to Start Month (Month YYYY)
                if not (ms and ws):
                    ms = ms or _approx_start_date_from_start_month(master_record.get("Start Month",""))
                    ws = ws or _approx_start_date_from_start_month(weekly_record.get("Start Month",""))

                pushed = bool(ms and ws and ws > ms)

                note = f"UPDATED vs Master ({', '.join(diffs)}) – Prod. #{baseline_index.get(k,0):03d} ({weekly_label})"
                if pushed:
                    note += " | Date pushed back"

                output_row = dict(weekly_record)
                output_row["Category"] = "Updated vs Master"
                output_row["Notes"] = note
                if pushed:
                    output_row["If It Was Pushed"] = "Yes"

                diff_rows_master.append(_weekly_to_master_projection(output_row, region, weekly_label))
            else:
                # No changes vs master → skip emitting a row
                continue

        else:
            # --- NEW to Master ---
            note = f"NEW to Master – Prod. #{baseline_index.get(k,0):03d} ({weekly_label})"
            output_row = dict(weekly_record)
            output_row["Category"] = "New to Master"
            output_row["Notes"] = note
            diff_rows_master.append(_weekly_to_master_projection(output_row, region, weekly_label))

    # Write out (Master schema)
    safe_region = (region or "All").replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "_")
    out_master = outdir / f"PW_{latest_date_for_filename}_VS_MASTER_{safe_region}.csv"

    with out_master.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=MASTER_SCHEMA, extrasaction="ignore")
        w.writeheader()
        w.writerows(diff_rows_master)

    print(f"[master-compare] Wrote: {out_master}")

    # --- Summary / metrics ---
    def _truthy(v: str) -> bool:
        return (v or "").strip().lower() in {"y", "yes", "true", "1"}

    total_pw = len(base_titles)  # includes filtered productions (from weekly_baseline)
    pushed_count = sum(1 for r in diff_rows_master if _truthy(r.get("Date Pushed Back?","")))

    if summary_acc is not None:
        # Batch mode: accumulate metrics instead of writing a per-region summary
        if "total_pw" not in summary_acc:
            summary_acc["total_pw"] = total_pw
        summary_acc["pushed_total"] = summary_acc.get("pushed_total", 0) + pushed_count
        files = summary_acc.setdefault("files", [])
        files.append({
            "region": region or "All Regions",
            "weekly_csv": weekly_csv.name,
            "weekly_baseline": Path(weekly_baseline).name,
            "master_compare": out_master.name,
        })
    else:
        # Single-region / CLI: keep existing per-region summary behavior
        summary_txt = outdir / f"PW_{latest_date_for_filename}_SUMMARY.txt"
        summary_txt.write_text(
            "\n".join([
                f"Production Weekly Summary — {weekly_label} ({region or 'All Regions'})",
                "",
                f"Total productions this issue (including filtered): {total_pw}",
                f"Productions with DATE PUSHED BACK: {pushed_count}",
                "",
                "Files:",
                f"- Weekly FullSchema: {weekly_csv.name}",
                f"- Baseline (incl. filtered): {Path(weekly_baseline).name}",
                f"- Master Compare: {out_master.name}",
            ]),
            encoding="utf-8",
        )
        print(f"[summary] Wrote: {summary_txt}")

def find_master_csv(master_dir: Path, region: str) -> Path:
    # Map dropdown region to expected filename suffix
    region_key = REGION_FILE_MAP.get(region, region)
    target = region_key.lower().replace(" ", "").replace("_", "").replace("-", "")
    for f in master_dir.glob("*.csv"):
        stem = f.stem.lower().replace(" ", "").replace("_", "").replace("-", "")
        if target in stem:
            return f
    raise FileNotFoundError(f"No master CSV found in {master_dir} matching region '{region}'")


# --------- World gazetteer (offline) ----------
_gc = geonamescache.GeonamesCache()
_GC_CITIES = _gc.get_cities()            # ~180k cities
_GC_CNTRY  = _gc.get_countries()         # keyed by ISO2

COMMON_FIXES = {
    "ontartio": "ontario",
    "los angles": "los angeles",
    "newyork": "new york",
    "New York": "new york",
    "new jersey": "new jersey",
}

@lru_cache(maxsize=1)
def _iso2_to_admin1_map():
    m = {}
    for subdiv in pycountry.subdivisions:
        iso2, adm = subdiv.code.split("-", 1)  # e.g., US-CA
        m.setdefault(iso2, {})[adm] = subdiv.name
    return m

@lru_cache(maxsize=1_000_000)
def _city_candidates(name_lower: str):
    return [c for c in _GC_CITIES.values() if c["name"].lower() == name_lower]

def _normalize_admin1(iso2: str, admin1_code: str) -> str:
    code = (admin1_code or "").split(".")[-1]  # GeoNames variants: "CA.06", "06", "NSW"
    adm_map = _iso2_to_admin1_map().get(iso2, {})
    if code in adm_map:
        return code if len(code) <= 3 else adm_map[code]
    return adm_map.get(code, code)

def world_lookup_city(city_raw: str, region_hint: str = "", country_hint: str = "") -> Tuple[str,str,str]:
    """
    Return (City, Region/State, Country) using global gazetteer.
    Exact match first; then fuzzy by name (>=90).
    """
    if not city_raw:
        return ("","","")
    q = city_raw.strip().lower()

    cands = _city_candidates(q)
    if not cands:
        best_name, best_score = None, 0
        for c in _GC_CITIES.values():
            s = fuzz.partial_ratio(q, c["name"].lower())
            if s > best_score:
                best_score, best_name = s, c["name"].lower()
                if s >= 98: break
        if best_score >= 90:
            cands = _city_candidates(best_name)
        else:
            return ("","","")

    def cand_score(c):
        score = 0
        if country_hint:
            if country_hint.strip().lower() in (
                _GC_CNTRY.get(c["countrycode"],{}).get("name","").lower(),
                c["countrycode"].lower()
            ):
                score += 2
        if region_hint:
            adm = _normalize_admin1(c["countrycode"], str(c.get("admin1code","")))
            if region_hint.strip().lower() in (adm.lower(),):
                score += 1
        return score

    cands.sort(key=cand_score, reverse=True)
    hit = cands[0]
    iso2 = hit["countrycode"]
    country = _GC_CNTRY.get(iso2, {}).get("name","")
    admin1 = _normalize_admin1(iso2, str(hit.get("admin1code","")))
    region = admin1 if len(admin1) <= 3 else admin1
    return (hit["name"], region, country)

# --------- Regions / countries helpers ----------
MONTHS = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June","July","August","September","October","November","December"], 1)}

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS",
    "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC",
    "ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
}
CA_PROV = {"AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"}

FULL_STATE = { # US full -> abbr
    "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA","colorado":"CO","connecticut":"CT",
    "delaware":"DE","district of columbia":"DC","florida":"FL","georgia":"GA","hawaii":"HI","idaho":"ID","illinois":"IL",
    "indiana":"IN","iowa":"IA","kansas":"KS","kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD","massachusetts":"MA",
    "michigan":"MI","minnesota":"MN","mississippi":"MS","missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV",
    "new hampshire":"NH","new jersey":"NJ","new mexico":"NM","new york":"NY","north carolina":"NC","north dakota":"ND",
    "ohio":"OH","oklahoma":"OK","oregon":"OR","pennsylvania":"PA","rhode island":"RI","south carolina":"SC","south dakota":"SD",
    "tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT","virginia":"VA","washington":"WA","west virginia":"WV",
    "wisconsin":"WI","wyoming":"WY"
}
FULL_PROV = { # CA full -> abbr
    "alberta":"AB","british columbia":"BC","manitoba":"MB","new brunswick":"NB","newfoundland and labrador":"NL",
    "nova scotia":"NS","northwest territories":"NT","nunavut":"NU","ontario":"ON","prince edward island":"PE",
    "quebec":"QC","saskatchewan":"SK","yukon":"YT","newfoundland": "NL", "newfoundland and labrador": "NL",
}

AU_STATES = {"NSW","VIC","QLD","WA","SA","TAS","ACT","NT"}
FULL_AU_STATE = {
    "new south wales":"NSW","victoria":"VIC","queensland":"QLD","western australia":"WA",
    "south australia":"SA","tasmania":"TAS","australian capital territory":"ACT","northern territory":"NT",
}

CITY_TO_REGION_COUNTRY = {
    # USA (key hubs)
    "los angeles": ("Los Angeles","CA","USA"),
    "atlanta": ("Atlanta","GA","USA"),
    "new york": ("New York","NY","USA"),
    "new york city": ("New York","NY","USA"),
    "brooklyn": ("Brooklyn","NY","USA"),
    "albuquerque": ("Albuquerque","NM","USA"),
    "chicago": ("Chicago","IL","USA"),
    # Canada (BC + ON + QC + others)
    "vancouver": ("Vancouver","BC","Canada"),
    "burnaby": ("Burnaby","BC","Canada"),
    "richmond": ("Richmond","BC","Canada"),
    "surrey": ("Surrey","BC","Canada"),
    "langley": ("Langley","BC","Canada"),
    "victoria": ("Victoria","BC","Canada"),
    "toronto": ("Toronto","ON","Canada"),
    "mississauga": ("Mississauga","ON","Canada"),
    "hamilton": ("Hamilton","ON","Canada"),
    "ottawa": ("Ottawa","ON","Canada"),
    "montreal": ("Montreal","QC","Canada"),
    "quebec city": ("Quebec City","QC","Canada"),
    "calgary": ("Calgary","AB","Canada"),
    "edmonton": ("Edmonton","AB","Canada"),
    "winnipeg": ("Winnipeg","MB","Canada"),
    "halifax": ("Halifax","NS","Canada"),
    "st. johns": ("St. John's","NL","Canada"),
    "st johns": ("St. John's","NL","Canada"),
    # UK/EU/NZ/AU (selection)
    "london, england": ("London","England","United Kingdom"),
    "sydney": ("Sydney","NSW","Australia"),
    "melbourne": ("Melbourne","VIC","Australia"),
    "brisbane": ("Brisbane","QLD","Australia"),
    "gold coast": ("Gold Coast","QLD","Australia"),
    "perth": ("Perth","WA","Australia"),
    "adelaide": ("Adelaide","SA","Australia"),
    "canberra": ("Canberra","ACT","Australia"),
    "hobart": ("Hobart","TAS","Australia"),
    "darwin": ("Darwin","NT","Australia"),
    "auckland": ("Auckland","","New Zealand"),
    "wellington": ("Wellington","","New Zealand"),
    "queenstown": ("Queenstown","","New Zealand"),
    "christchurch": ("Christchurch","","New Zealand"),
    
    # Key capitals / problem cases
    "bangkok": ("Bangkok", "", "Thailand"),
    "cairo": ("Cairo", "", "Egypt"),
    "tokyo": ("Tokyo", "", "Japan"),
    "kyoto": ("Kyoto", "", "Japan"),
    "osaka": ("Osaka", "", "Japan"),
    "st. johns": ("St. John's", "NL", "Canada"),
    "st johns": ("St. John's", "NL", "Canada"),
    "japan": ("", "", "Japan"),



}

HUB_SUBSTRINGS = [
    (re.compile(r"\blos\s+angeles\b", re.I), ("Los Angeles","CA","USA")),
    (re.compile(r"\bnew\s+york\b", re.I),    ("New York","NY","USA")),
    (re.compile(r"\batlanta\b", re.I),       ("Atlanta","GA","USA")),
    (re.compile(r"\balbuquerque\b", re.I),   ("Albuquerque","NM","USA")),
    (re.compile(r"\bchicago\b", re.I),       ("Chicago","IL","USA")),

    (re.compile(r"\bvancouver\b", re.I),     ("Vancouver","BC","Canada")),
    (re.compile(r"\brichmond\b", re.I),      ("Richmond","BC","Canada")),
    (re.compile(r"\bburnaby\b", re.I),       ("Burnaby","BC","Canada")),
    (re.compile(r"\bsurrey\b", re.I),        ("Surrey","BC","Canada")),
    (re.compile(r"\blangley\b", re.I),       ("Langley","BC","Canada")),
    (re.compile(r"\bvictoria\b", re.I),      ("Victoria","BC","Canada")),
    (re.compile(r"\btoronto\b", re.I),       ("Toronto","ON","Canada")),
    (re.compile(r"\bmississauga\b", re.I),   ("Mississauga","ON","Canada")),
    (re.compile(r"\bhamilton\b", re.I),      ("Hamilton","ON","Canada")),
    (re.compile(r"\bottawa\b", re.I),        ("Ottawa","ON","Canada")),
    (re.compile(r"\bmontreal\b", re.I),      ("Montreal","QC","Canada")),
    (re.compile(r"\bcalgary\b", re.I),       ("Calgary","AB","Canada")),
    (re.compile(r"\bwinnipeg\b", re.I),      ("Winnipeg","MB","Canada")),
    (re.compile(r"\bhalifax\b", re.I),       ("Halifax","NS","Canada")),

    (re.compile(r"\blondon,\s*england\b", re.I), ("London","England","United Kingdom")),
    (re.compile(r"\bsydney\b", re.I),        ("Sydney","NSW","Australia")),
    (re.compile(r"\bmelbourne\b", re.I),     ("Melbourne","VIC","Australia")),
]


COUNTRY_ALIASES = {
    # USA
    "usa":"USA","u.s.a.":"USA","u.s.":"USA","us":"USA","united states":"USA","united states of america":"USA","america":"USA",
    # Canada
    "canada":"Canada","can.":"Canada","can":"Canada",  # intentionally NOT "ca" (conflicts with California)
    # UK & Ireland
    "united kingdom":"United Kingdom","u.k.":"United Kingdom","uk":"United Kingdom",
    "england":"United Kingdom","scotland":"United Kingdom","wales":"United Kingdom","northern ireland":"United Kingdom",
    # Oceania
    "australia":"Australia","aus":"Australia",
    "new zealand":"New Zealand","nz":"New Zealand",
    # Common film hubs
    "ireland":"Ireland","hungary":"Hungary","poland":"Poland","czech republic":"Czech Republic","czechia":"Czech Republic",
    "france":"France","germany":"Germany","spain":"Spain","italy":"Italy","portugal":"Portugal",
    "thailand":"Thailand","egypt":"Egypt","japan":"Japan","korea":"South Korea","south korea":"South Korea",
}

EU_COUNTRIES = {
    "United Kingdom","Ireland","France","Germany","Spain","Italy","Netherlands","Belgium","Austria","Switzerland",
    "Czech Republic","Poland","Romania","Bulgaria","Denmark","Sweden","Norway","Finland","Iceland","Portugal","Greece",
    "Slovakia","Slovenia","Croatia","Lithuania","Latvia","Estonia","Luxembourg","Malta","Hungary","Serbia"
}

# Common one-off spelling fixes for locations seen in PW text
COMMON_FIXES = {
    "ontartio": "ontario",
    "los angles": "los angeles",
    "newyork": "new york",
    "united kindom": "united kingdom",
    "united kngdom": "united kingdom",
    "tokoyo": "tokyo",
    "munchen": "munich",
    "prauge": "prague",
}




def _fix_common_typos(s: str) -> str:
    """Return a corrected version for known typos; otherwise original string."""
    if not s:
        return s
    key = s.strip().lower()
    return COMMON_FIXES.get(key, s)


def _normalize_spaces(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "").strip())

def _as_country(token: str) -> str:
    t = (token or "").strip().lower()
    return COUNTRY_ALIASES.get(t, "")

def _looks_like_region_token(token: str) -> Tuple[str,str]:
    if not token: return ("","")
    up = token.strip().upper()
    lo = token.strip().lower()
    if up in US_STATES: return (up, "USA")
    if up in CA_PROV:   return (up, "Canada")
    if up in AU_STATES: return (up, "Australia")
    if lo in FULL_STATE:    return (FULL_STATE[lo], "USA")
    if lo in FULL_PROV:     return (FULL_PROV[lo], "Canada")
    if lo in FULL_AU_STATE: return (FULL_AU_STATE[lo], "Australia")
    return ("","")

def _city_state_country_from_location(loc_val: str) -> Tuple[str, str, str]:
    """
    Parse LOCATION(S) into (City, Region/State, Country).
    Robust to: slashes/amps/dashes, parentheses, typos, pure country tokens,
    province/state-only tokens, hub substrings, hard dictionary, and gazetteer fallback.
    Multiple locations are joined with " + " (leave as-is for downstream code).
    """
    if not loc_val:
        return ("","","")

    # Normalize punctuation & trim
    raw = (loc_val or "")
    raw = re.sub(r"[–—]", "-", raw)
    raw = raw.replace("’","'").replace("‘","'").replace("`","'")
    raw = re.sub(r"\s*\([^)]*\)\s*", " ", raw)  # drop (...notes)
    raw = re.sub(r"\s+", " ", raw).strip()

    # Pure country fast-path (before splitting)
    ctry_only = _as_country(raw.lower())
    if ctry_only:
        return ("","", ctry_only)

    # Split into multi-locations on common separators (keep “ - ” as a splitter)
    parts = re.split(r"\s*(?:[;/|&]|-\s+|\s+-)\s*", raw)
    parts = [p.strip() for p in parts if p.strip()]
    if not parts:
        return ("","","")

    parsed: list[Tuple[str,str,str]] = []

    for p in parts:
        p = _fix_common_typos(p)
        p = re.sub(r"\s+", " ", p).strip()
        low = p.lower(); up = p.upper()

        # Country token?
        ctry = _as_country(low)
        if ctry:
            parsed.append(("", "", ctry)); continue

        # Province/state token alone?
        reg_abbr, inf_ctry = _looks_like_region_token(p)
        if reg_abbr and ("," not in p):
            parsed.append(("", reg_abbr, inf_ctry)); continue

        # Hub substring overrides (fast path)
        matched = False
        for rx, triple in HUB_SUBSTRINGS:
            if rx.search(p):
                parsed.append(triple); matched = True; break
        if matched:
            continue

        # Hard dictionary exact (lowercased keys)
        if low in CITY_TO_REGION_COUNTRY:
            parsed.append(CITY_TO_REGION_COUNTRY[low]); continue

        # Comma forms
        toks = [t.strip() for t in p.split(",") if t.strip()]
        if len(toks) == 3:
            city, regtok, ctrytok = toks
            reg_abbr, inf_ctry = _looks_like_region_token(regtok)
            region = reg_abbr or regtok
            ctry = _as_country(ctrytok.lower()) or inf_ctry or ctrytok
            parsed.append((_normalize_spaces(city), _normalize_spaces(region), _normalize_spaces(ctry)))
            continue

        if len(toks) == 2:
            city, tail = toks
            city = _normalize_spaces(city)
            tail = _normalize_spaces(tail)
            # tail could be a country…
            maybe_ctry = _as_country(tail.lower())
            if maybe_ctry:
                parsed.append((city, "", maybe_ctry)); continue
            # …or a region/province/state
            reg_abbr, inf_ctry = _looks_like_region_token(tail)
            if reg_abbr:
                parsed.append((city, reg_abbr, inf_ctry)); continue
            # unknown tail -> keep city only
            parsed.append((city, "", "")); continue

        # Gazetteer fallback (handles bare city names)
        c2, r2, k2 = world_lookup_city(p)
        if k2:
            if r2.isdigit():  # drop numeric admin codes
                r2 = ""
            parsed.append((_normalize_spaces(c2 or p), _normalize_spaces(r2), k2))
        else:
            parsed.append((_normalize_spaces(p), "", ""))

    # Join multiples with +
    city    = " + ".join([c for c,_,_ in parsed if c])
    region  = " + ".join([r for _,r,_ in parsed if r])
    country = " + ".join([ct for *_,ct in parsed if ct])

    return (city, region, country)

# def _city_state_country_from_location(loc_val: str) -> Tuple[str, str, str]:
#     """
#     Parse LOCATION(S) into (City, Region, Country).
#     Handles multi-locations, apostrophes, pure countries, and gazetteer fallback.
#     """
#     if not loc_val:
#         return ("","","")

#     # Normalize apostrophes and dashes
#     raw = (loc_val or "").replace("—","-").replace("–","-").replace("’","'").strip()

#     # Quick pure country path (before splitting!)
#     low_raw = raw.lower()
#     ctry = _as_country(low_raw)
#     if ctry:
#         return ("", "", ctry)

#     # Split into multiple locations (only break on dashes with space around them)
#     parts = re.split(r"\s*[;/|]\s*|\s+-\s+", raw)

#     parts = [p.strip() for p in parts if p.strip()]
#     if not parts:
#         return ("","","")

#     parsed = []
#     for p in parts:
#         p = _fix_common_typos(p)
#         low = p.lower()
#         up  = p.upper()
#         p = p.replace("’","'").replace("‘","'").replace("`","'")
#         p = p.strip()

#         # Pure country check (fixes Japan→Apan bug)
#         ctry = _as_country(p.lower())
#         if ctry:
#             parsed.append(("", "", ctry))
#             continue
#         if not p:
#             continue

#         # 1. Country-only token
#         ctry = _as_country(low)
#         if ctry:
#             parsed.append(("", "", ctry))
#             continue

#         # 2. Pure state/province
#         if up in US_STATES or low in FULL_STATE:
#             parsed.append(("", FULL_STATE.get(low, up), "USA"))
#             continue
#         if up in CA_PROV or low in FULL_PROV:
#             parsed.append(("", FULL_PROV.get(low, up), "Canada"))
#             continue
#         if up in AU_STATES or low in FULL_AU_STATE:
#             parsed.append(("", FULL_AU_STATE.get(low, up), "Australia"))
#             continue

#         # 3. Hub overrides
#         for rx, triple in HUB_SUBSTRINGS:
#             if rx.search(p):
#                 parsed.append(triple)
#                 break
#         else:
#             # 4. Hard dictionary
#             if low in CITY_TO_REGION_COUNTRY:
#                 parsed.append(CITY_TO_REGION_COUNTRY[low])
#                 continue

#             # 5. Comma form
#             toks = [t.strip() for t in p.split(",") if t.strip()]
#             if len(toks) == 3:
#                 city, regtok, ctrytok = toks
#                 reg_abbr, inf_ctry = _looks_like_region_token(regtok)
#                 region = reg_abbr or regtok
#                 ctry = _as_country(ctrytok) or inf_ctry or ctrytok
#                 parsed.append((_normalize_spaces(city), region, ctry))
#                 continue
#             elif len(toks) == 2:
#                 city, tail = toks

#                 # normalize quotes and whitespace
#                 city = _normalize_spaces(city.replace("’","'").replace("‘","'").replace("`","'"))
#                 tail_norm = _normalize_spaces(tail.replace("’","'").replace("‘","'").replace("`","'"))

#                 # 1. Check if the tail is a country
#                 maybe_ctry = _as_country(tail_norm.lower())
#                 if maybe_ctry:
#                     parsed.append((city, "", maybe_ctry))
#                     continue

#                 # 2. Otherwise, check if it's a region (state/province)
#                 reg_abbr, inf_ctry = _looks_like_region_token(tail_norm)
#                 if reg_abbr:
#                     parsed.append((city, reg_abbr, inf_ctry))
#                 else:
#                     # 3. Fallback: keep city only
#                     parsed.append((city, "", ""))

#                 continue


#             # 6. Gazetteer fallback
#             c2, r2, k2 = world_lookup_city(p)
#             if k2:
#                 if r2.isdigit():  # drop codes like "09"
#                     r2 = ""
#                 parsed.append((_normalize_spaces(c2 or p), _normalize_spaces(r2), k2))
#             else:
#                 # If still nothing, keep raw as city
#                 parsed.append((_normalize_spaces(p), "", ""))

#     # Join multiples with +
#     city    = " + ".join([c for c,_,_ in parsed if c])
#     region  = " + ".join([r for _,r,_ in parsed if r])
#     country = " + ".join([ct for *_,ct in parsed if ct])

#     return (city, region, country)


def region_bucket(city: str, region: str, country: str) -> str:
    """
    Map (City, Region/State, Country) -> reporting buckets.
    Uses only the FIRST location if multiples (“X + Y”).
    Infers country from region/province codes when needed.
    """
    first_city    = (city.split("+")[0] if city else "").strip()
    first_region  = (region.split("+")[0] if region else "").strip().upper()
    first_country = (country.split("+")[0] if country else "").strip()

    # Normalize country (or infer from region)
    ctry = _as_country(first_country.lower()) or first_country
    if not ctry and first_region:
        if first_region in US_STATES or first_region.lower() in FULL_STATE:
            ctry = "USA"
        elif first_region in CA_PROV or first_region.lower() in FULL_PROV:
            ctry = "Canada"
        elif first_region in AU_STATES or first_region.lower() in FULL_AU_STATE:
            ctry = "Australia"

    # --- Canada split per your rule ---
    if ctry == "Canada":
        if first_region == "BC":
            return "West Coast Canada"
        if first_region == "QC":
            return "Quebec"

        # Everything else in Canada → East Coast Canada
        return "East Coast Canada"

    # USA unchanged
    if ctry in {"USA","United States"}:
        return "United States"

    # Other existing buckets unchanged
    if ctry in {"Ireland","Hungary"}:
        return "Ireland/Hungary"
    if ctry in {"Australia","New Zealand"}:
        return "Australia/New Zealand"
    if ctry in EU_COUNTRIES:
        return "Europe/Other"
    return "Other"






# --------- Regexes ----------
RE_EMAIL = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
RE_PHONE = re.compile(r"(?:\+?\d{1,2}[-.\s]?)?(?:\(?\d{3}\)?)[-.\s]?\d{3}[-.\s]?\d{4}\b")
RE_STATUS = re.compile(r"\bSTATUS:\s*(.+?)(?:\s{2,}|$)", re.I)
RE_LOCATION = re.compile(r"\bLOCATION(?:S|\(S\))?:\s*(.+?)(?:\s{2,}|$)",re.I)
RE_LABEL_LINE = re.compile(r"\b([A-Z][A-Z/]{1,30}):\s*(.+)$")
# Flexible span inside or outside parentheses, with optional year on the first date.
RE_DATE_RANGE = re.compile(
    r"([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?\s*[-–]\s*"
    r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})",
    re.I,
)

EXCLUDE_KEYWORDS = ["telefilm", "hallmark", "gaf", "great american family"]

def fill_na(row: dict, fields=SCHEMA, na="N/A") -> dict:
    out = dict(row)
    for k in fields:
        v = out.get(k, "")
        if v is None or (isinstance(v, str) and v.strip() == ""):
            out[k] = na
    return out

def _first_email(txt: str) -> str:
    m = RE_EMAIL.search(txt or ""); return m.group(0) if m else ""

def _first_phone(txt: str) -> str:
    m = RE_PHONE.search(txt or ""); return m.group(0) if m else ""

def _quoted_title_from_title_line(line: str) -> str:
    s = line.strip()
    if s.startswith(TITLE_PREFIX):
        s = s[len(TITLE_PREFIX):].lstrip()
    m = re.search(r'^[“"]\s*([^”"]+?)\s*[”"]', s)
    return m.group(1).strip() if m else s.split("  ")[0].strip()

def _type_from_title_line(line: str) -> str:
    s = line.lower()
    if "feature film" in s or re.search(r"\bfeature\b", s): return "Feature Film"
    return "Television"

def _format_label_from_title_line(line: str) -> str:
    s = line.strip()
    if s.startswith(TITLE_PREFIX): s = s[len(TITLE_PREFIX):].lstrip()
    s = re.sub(r"\s+\d{2}-\d{2}-\d{2}\s*ê?$", "", s)
    m = re.match(r'^[“"]\s*([^”"]+?)\s*[”"]\s*(.+)$', s)
    tail = m.group(2) if m else ""
    if not tail: return ""
    before_slash = tail.split("/", 1)[0].strip().lower()
    season = ""
    m_sea = re.search(r"\bseason\s+(\d{1,2})\b", before_slash, re.I) or re.search(r"\bS(\d{1,2})\b", before_slash, re.I)
    if m_sea: season = f"Season {m_sea.group(1)}"
    if re.search(r"\blimited\b.*\bseries\b", before_slash): label = "Limited Series"
    elif re.search(r"\bmini[-\s]?series\b", before_slash):  label = "Mini-Series"
    elif re.search(r"\banthology\b", before_slash):         label = "Anthology Series"
    elif re.search(r"\b(docu[\s-]?series|documentary\s+series)\b", before_slash): label = "Docuseries"
    elif re.search(r"\btelefilm\b|\btv\s*movie\b", before_slash): label = "Telefilm"
    elif re.search(r"\bpilot\b", before_slash):              label = "Pilot"
    elif re.search(r"\banimated\b.*\bseries\b", before_slash): label = "Animated Series"
    elif re.search(r"\blive[-\s]?action\b.*\bseries\b", before_slash): label = "Live-Action Series"
    elif re.search(r"\bfeature(\s+film)?\b", before_slash): label = "Feature Film"
    elif re.search(r"\bseries\b", before_slash):             label = "Series"
    else: label = ""
    return f"{label} ({season})" if label == "Series" and season else label

def _parse_date_range(block_text: str) -> Tuple[str, Optional[date], Optional[date]]:
    """
    Parse spans like:
      - March 2 - April 7, 2027
      - March 2, 2026 - April 7, 2027
      - (March 2 - April 7, 2027)
    anywhere in the block text.

    If the first year is omitted, we assume it's the same as the end year.
    If the computed end < start, we treat it as crossing New Year.
    """
    txt = (block_text or "")
    m = RE_DATE_RANGE.search(txt)
    if not m:
        return "", None, None

    m1, d1, y1, m2, d2, y2 = m.groups()
    d1, d2, y2 = int(d1), int(d2), int(y2)
    start_year = int(y1) if y1 else y2

    try:
        sd = date(start_year, MONTHS[m1.lower()], d1)
        ed = date(y2,         MONTHS[m2.lower()], d2)
        if ed < sd:
            sd = date(y2 - 1, MONTHS[m1.lower()], d1)
        span_text = f"{m1} {d1} – {m2} {d2}, {ed.year}"
        return span_text, sd, ed
    except Exception:
        # if something weird happens, at least return the matched text
        return m.group(0), None, None


def _inclusive_days(sd: Optional[date], ed: Optional[date]) -> str:
    if not sd or not ed: return ""
    return str((ed - sd).days + 1)

def _active_today(sd: Optional[date], ed: Optional[date]) -> str:
    if not sd or not ed: return ""
    t = date.today(); return "Yes" if (sd <= t <= ed) else "No"

def _gather_director_producer(block_lines: List[str]) -> str:
    out=[]
    for ln in block_lines:
        m = RE_LABEL_LINE.search(ln)
        if not m: continue
        label, val = m.group(1).upper(), m.group(2).strip()
        if label in ("DIRECTOR","PRODUCER","SHOWRUNNER"):
            out.append(f"{label.title()}: {val}")
    return " | ".join(out)

def _gather_vfx(block_lines: List[str]) -> str:
    return " | ".join(_normalize_spaces(ln) for ln in block_lines if re.search(r"\bVFX\b|\bVisual Effects\b", ln, re.I))

def _gather_studio_info(block_lines: List[str]) -> str:
    return " | ".join(_normalize_spaces(ln) for ln in block_lines if re.search(r"\b(STUDIO|PICTURES|TELEVISION)\b", ln, re.I))

def _addressy(ln: str) -> bool:
    return bool(re.search(r"\d{2,} .*(St\.|Street|Rd\.|Road|Ave\.|Avenue|Blvd\.|Boulevard|Suite|Floor|#)", ln, re.I))

def _upperish(ln: str) -> bool:
    letters=[c for c in ln if c.isalpha()]
    return bool(letters) and (sum(1 for c in letters if c.isupper())/len(letters) >= 0.6)

def to_master_row(row: dict, issue_link: str = "") -> dict:
    days = (row.get("Computed Production Length","") or "").strip()
    months = ""
    if days.isdigit():
        d = int(days); months = f"{d//30}.{d%30:02d}"
    city = row.get("City",""); region = row.get("Province/State",""); country = row.get("Country","")
    bucket = region_bucket(city, region, country)
    return {
        "Region Bucket": bucket,
        "Category": row.get("Category",""),
        "Production Name": row.get("Production Name",""),
        "Issue Link": issue_link,
        "Start Month": row.get("Start Month",""),
        "Shooting Dates": row.get("Shooting Dates",""),
        "Actively in Production": row.get("Actively in Production",""),
        "Date Pushed Back?": row.get("If It Was Pushed",""),
        "Length (Days)": days,
        "Description": row.get("Description",""),
        "City": city,
        "Province/State": region,
        "Country": country,
        "Type": row.get("Type",""),
        "Director/Producer": row.get("Director/Producer",""),
        "VFX Notes": row.get("VFX Team",""),
        "IMDb Link": "",
        "Studio Name": row.get("Studio Info",""),
        "Production Office": row.get("Production Office",""),
        "Production Phone/Email": f"{row.get('Production Phone','')} {row.get('Production Email','')}".strip(),
        "Prod. Co": detect_prod_co(row), 
    }

KNOWN_PROD_CO_MAPPING = {
    "disney": "Disney",
    "marvel studios": "Marvel Studios",
    "lucasfilm": "Lucasfilm",
    "20th century studios": "20th Century Studios",
    "warner bros. television": "Warner Bros.", # Specific mapping for variations
    "warner bros. pictures": "Warner Bros.",
    "warner bros. entertainment": "Warner Bros.",
    "warner bros.": "Warner Bros.",
    "new line cinema": "New Line Cinema",
    "dc studios": "DC Studios",
    "universal pictures": "Universal Pictures",
    "universal television": "Universal Pictures", # Specific mapping
    "focus features": "Focus Features",
    "dreamworks": "DreamWorks",
    "sony pictures animation": "Sony Pictures", # Specific mapping
    "sony pictures": "Sony Pictures",
    "columbia pictures": "Sony Pictures", # Canonical under Sony
    "tristar pictures": "Sony Pictures", # Canonical under Sony
    "tristar": "Sony Pictures",
    "screen gems": "Sony Pictures", # Canonical under Sony
    "paramount pictures": "Paramount Pictures",
    "nickelodeon movies": "Paramount Pictures", # Canonical under Paramount
    "paramount animation": "Paramount Pictures", # Canonical under Paramount
    "amazon mgm studios": "Amazon MGM Studios",
    "mgm": "Amazon MGM Studios", # Canonical under Amazon MGM
    "orion pictures": "Amazon MGM Studios", # Canonical under Amazon MGM
    "orion": "Amazon MGM Studios",
    "netflix studios": "Netflix", # Specific mapping
    "netflix": "Netflix",
    "apple original films": "Apple",
    "apple tv+": "Apple",
    "apple studios": "Apple", # Adding this from your example
    "apple": "Apple",
    "blumhouse productions": "Blumhouse Productions",
    "atomic monster": "Atomic Monster",
    "legendary entertainment": "Legendary Entertainment",
    "bad robot": "Bad Robot",
    "skydance media": "Skydance", # Specific mapping
    "skydance": "Skydance",
    "lionsgate films": "Lionsgate", # Specific mapping
    "lionesgate": "Lionsgate", # Typo fix
    "summit entertainment": "Summit Entertainment", # Often under Lionsgate, but keep separate if distinct
    "a24": "A24",
    "toho company": "Toho",
    "toho": "Toho",
    "cj enm": "CJ ENM",
    "tencent pictures": "Tencent Pictures",
    "proximity productions llc": "Proximity Productions", # From your example
    "doozer productions": "Doozer Productions", # From your example
    "two soups productions": "Two Soups Productions" # From your example
    
}

KNOWN_PROD_CO_MAPPING.update({
    "amazon studios": "Amazon Studios",
    "playstation productions": "PlayStation Productions",
})



def detect_prod_co(row: dict) -> str:
    """
    Detects known production companies by searching relevant text fields.
    Returns a '+' separated string of unique canonical company names found.
    """
    # Combine relevant text fields for searching
    search_text = " ".join([
        row.get("Description",""),
        row.get("Studio Info",""),
        row.get("Production Company",""),
        row.get("Director/Producer", ""), # Sometimes production companies are listed here
    ]).lower()
    # Iterate through the mapping from specific search terms to canonical names
    # Iterate by length of key DESCENDING to prioritize longer, more specific matches
    # e.g., "warner bros. television" before "warner bros."
    sorted_keys = sorted(KNOWN_PROD_CO_MAPPING.keys(), key=len, reverse=True)

    found_companies = set()

    for term_lower in sorted_keys:
        if term_lower in search_text:
            canonical_name = KNOWN_PROD_CO_MAPPING[term_lower]
            found_companies.add(canonical_name)

    return "+".join(sorted(found_companies))


# ---------- LOAD BLOCKS ----------
def _load_from_structured(path: Path) -> List[str]:
    txt = path.read_text(encoding="utf-8", errors="ignore")
    chunks = [c.strip() for c in txt.split(PROD_BREAK)]
    blocks=[]
    for c in chunks:
        lines=[ln for ln in c.splitlines() if ln.strip()]
        if not lines: continue
        if lines[0].lstrip().startswith(TITLE_PREFIX) or lines[0].lstrip().startswith(("“",'"')):
            blocks.append("\n".join(lines))
    return blocks

def _load_from_productions_folder(path: Path) -> List[str]:
    files = sorted(path.glob("*_prod*.txt"))
    blocks=[]
    for f in files:
        text = f.read_text(encoding="utf-8", errors="ignore")
        if text.strip(): blocks.append(text.strip())
    return blocks

def load_blocks(input_path: Path) -> Tuple[List[str], List[str]]:
    if input_path.is_dir():
        prod_dir = input_path / "productions"
        blocks = _load_from_productions_folder(prod_dir if prod_dir.exists() else input_path)
    else:
        blocks = _load_from_structured(input_path)
    titles=[]
    for b in blocks:
        lines=[ln for ln in b.splitlines() if ln.strip()]
        title=_quoted_title_from_title_line(lines[0] if lines else "")
        titles.append(title)
    return blocks, titles

# ---------- BUILD ----------
def build_cmd(input_path: Path, outdir: Path, label: str):
    blocks, titles = load_blocks(input_path)
    outdir.mkdir(parents=True, exist_ok=True)
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("_")
    csv_path   = outdir / f"{safe_label}_FullSchema.csv"
    baseline   = outdir / f"{safe_label}_baseline_titles.txt"
    filtered   = outdir / f"{safe_label}_filtered_titles.txt"

    filtered_titles=[]; kept_rows=[]
    for b in blocks:
        row, title, is_excluded = _parse_block_to_row(b)
        if is_excluded:
            filtered_titles.append(title)
            continue
        kept_rows.append(fill_na(row))

    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=SCHEMA, extrasaction="ignore")
        w.writeheader(); w.writerows(kept_rows)

    baseline.write_text("\n".join(titles), encoding="utf-8")
    filtered.write_text("\n".join(filtered_titles), encoding="utf-8")

    print(f"[build] Wrote {len(kept_rows)} rows to {csv_path}")
    print(f"[build] Baseline (incl. filtered): {len(titles)} titles -> {baseline.name}")
    print(f"[build] Filtered titles ({len(filtered_titles)}): {filtered.name}")
    return str(csv_path), str(baseline), str(filtered)

# ---------- COMPARE ----------
def _read_csv_rows(path: Path) -> List[Dict[str,str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]

def _collapse_dupes(rows: List[dict]) -> Tuple[Dict[str, dict], List[Tuple[str, str]]]:
    order=[]; best={}; dupes=[]
    def score(row: dict) -> int:
        fields = ["Shooting Dates","Description","Director/Producer","All Locations","City","Province/State","Country"]
        return sum(1 for f in fields if (row.get(f) or "").strip() and (row.get(f) or "").strip() != "N/A")
    for r in rows:
        k = _norm_key(r.get("Production Name",""))
        if not k: continue
        if k not in best:
            best[k]=r; order.append(k)
        else:
            dupes.append((r.get("Production Name",""), best[k].get("Production Name","")))
            if score(r) > score(best[k]): best[k]=r
    return ({k: best[k] for k in order}, dupes)

def _read_titles_list(path: Path) -> List[str]:
    return [ln.strip() for ln in path.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]


def _start_date_from_shooting_dates(s: str) -> Optional[date]:
    """Derive a concrete start date from a Shooting Dates span.

    Works with both the older 5-group RE_DATE_RANGE:
        (Month d - Month d, yyyy)
    and the newer 6-group version:
        Month d, yyyy - Month d, yyyy
    where the first year may be optional.
    """
    m = RE_DATE_RANGE.search(s or "")
    if not m:
        return None

    groups = m.groups()
    try:
        if len(groups) == 5:
            # Old pattern: (Month1 day1 - Month2 day2, year)
            m1, d1, _, _, y = groups
        elif len(groups) == 6:
            # New pattern: Month1 day1, year1? - Month2 day2, year2
            m1, d1, y1, _, _, y2 = groups
            y = y1 or y2
        else:
            return None

        return date(int(y), MONTHS[m1.lower()], int(d1))
    except Exception:
        return None
    
def _approx_start_date_from_start_month(s: str) -> Optional[date]:
    """
    Approximate a concrete date from a 'Start Month' value.

    For values like "March 2026" we return date(2026, 3, 1) so that
    pushed-back detection can still work even when there is no explicit
    shooting date range. If there is no 4-digit year present, we return None.
    """
    if not s:
        return None

    t = (s or "").strip()
    m = re.match(
        r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
        t
    )
    if not m:
        return None

    month_name, year = m.groups()
    try:
        return date(int(year), MONTHS[month_name.lower()], 1)
    except Exception:
        return None



def _changed_fields(old: Dict[str,str], new: Dict[str,str]) -> List[str]:
    # Add "Production Name" to the list of fields to check for changes.
    fields = [
        "Production Name", "Shooting Dates", "Start Month", "City", "Province/State", "Country",
        "Type", "Format Label", "Director/Producer", "All Locations",
        "Production Office", "Production Company"
    ]
    return [f for f in fields if not _equiv((old.get(f,"") or ""), (new.get(f,"") or ""))]

def compare_cmd(old_csv: Path, new_csv: Path, old_baseline: Path, new_baseline: Path,
                old_label: str, new_label: str, outdir: Path, latest_date_for_filename: str):
    outdir.mkdir(parents=True, exist_ok=True)

    old_rows = _read_csv_rows(old_csv)
    new_rows = _read_csv_rows(new_csv)
    
    # Add a flag to track which old rows have been matched
    for row in old_rows:
        row['_matched'] = False

    out_rows = []
    updated_count = 0
    
    # --- Fuzzy Matching Logic ---
    # For each new production, find its best match in the old list.
    for n_row in new_rows:
        best_match_row = None
        best_score = 0
        
        # Use _norm_text to clean the title before comparing
        n_title_norm = _norm_text(n_row.get("Production Name", ""))

        for o_row in old_rows:
            # Skip rows that have already been matched to a new production
            if o_row.get('_matched'):
                continue
            
            o_title_norm = _norm_text(o_row.get("Production Name", ""))
            # Use token_set_ratio, which is good for finding subsets (e.g., "Shogun" in "Shogun S:2")
            score = fuzz.token_set_ratio(n_title_norm, o_title_norm)
            
            if score > best_score:
                best_score = score
                best_match_row = o_row

        # --- Apply Thresholds ---
        # Normalize the raw score to be out of 100
        score_percent = best_score

        if score_percent > 90:
            # > 90% match: CONFIDENT MATCH. Treat as the same production.
            best_match_row['_matched'] = True
            diffs = _changed_fields(best_match_row, n_row)
            if diffs:
                row = dict(n_row)
                row["Category"] = "Updated"
                row["Notes"] = f"UPDATED ({', '.join(diffs)})"
                out_rows.append(fill_na(row))
                updated_count += 1

        elif 50 <= score_percent <= 90:
            # 50-90% match: LIKELY A RENAMING OR MAJOR UPDATE. Flag as updated.
            best_match_row['_matched'] = True
            row = dict(n_row)
            row["Category"] = "Updated"
            old_name = best_match_row.get("Production Name", "")
            new_name = n_row.get("Production Name", "")
            row["Notes"] = f"UPDATED (Name changed from '{old_name}' to '{new_name}')"
            out_rows.append(fill_na(row))
            updated_count += 1
            
        else: # < 50% match
            # < 50% match: Treat as a NEW production.
            row = dict(n_row)
            row["Category"] = "New"
            row["Notes"] = f"NEW – from {new_label}"
            out_rows.append(fill_na(row))

    # After checking all new rows, any old rows not matched are REMOVED.
    for o_row in old_rows:
        if not o_row.get('_matched'):
            row = dict(o_row)
            row["Category"] = "Removed"
            row["Notes"] = f"REMOVED – from {old_label}"
            out_rows.append(fill_na(row))

    # ---- Write comparison CSVs ----
    out_csv = outdir / f"PW_{latest_date_for_filename}_Comparison_FullSchema.csv"
    with out_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=COMPARE_SCHEMA, extrasaction="ignore")
        w.writeheader(); w.writerows(out_rows)
    print(f"[compare] Wrote comparison CSV: {out_csv}")

    out_master = outdir / f"PW_{latest_date_for_filename}_Comparison_MasterSchema.csv"
    with out_master.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=MASTER_SCHEMA, extrasaction="ignore")
        w.writeheader()
        for r in out_rows:
            w.writerow(to_master_row(r, issue_link=new_label))
    print(f"[compare] Wrote master-schema CSV: {out_master}")
    
    # ---- Summary ----
    new_c = sum(1 for r in out_rows if r.get("Category") == "New")
    upd_c = sum(1 for r in out_rows if r.get("Category") == "Updated")
    rem_c = sum(1 for r in out_rows if r.get("Category") == "Removed")
    print("\n=== Summary ===")
    print(f"New: {new_c}")
    print(f"Updated: {upd_c}")
    print(f"Removed: {rem_c}")

def batch_build_cmd(input_dir: Path, out_root: Path, glob_pattern: str = "*.pdf",
                    skip_clean: bool = True, resume: bool = True) -> Path:
    """
    Process all PDFs in a folder → each gets its own subfolder and FullSchema build.
    Writes a batch_index.csv in out_root summarizing results.
    Returns the path to batch_index.csv.
    """
    input_dir = Path(input_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(input_dir.glob(glob_pattern))
    if not pdfs:
        raise FileNotFoundError(f"No PDFs matched {glob_pattern!r} in {input_dir}")

    rows = []
    for pdf in pdfs:
        per_pdf_out = out_root / pdf.stem
        fullschema = per_pdf_out / f"{pdf.stem}_FullSchema.csv"

        if resume and fullschema.exists():
            # Skip work; just record existing result
            try:
                import pandas as pd
                nrows = len(pd.read_csv(fullschema, dtype=str, encoding="utf-8-sig"))
            except Exception:
                nrows = ""
            rows.append({
                "PDF": str(pdf),
                "OutDir": str(per_pdf_out),
                "FullSchema": str(fullschema),
                "Baseline": str(per_pdf_out / f"{pdf.stem}_baseline_titles.txt"),
                "Filtered": str(per_pdf_out / f"{pdf.stem}_filtered_titles.txt"),
                "Rows": nrows,
                "Status": "skipped (resume)"
            })
            continue

        try:
            csv_path, baseline_path, per_dir = pipeline_build_one(pdf, out_root, skip_clean=skip_clean)
            # count rows quickly (optional)
            try:
                import pandas as pd
                nrows = len(pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig"))
            except Exception:
                nrows = ""
            rows.append({
                "PDF": str(pdf),
                "OutDir": str(per_dir),
                "FullSchema": str(csv_path),
                "Baseline": str(baseline_path),
                "Filtered": str(per_dir / f"{pdf.stem}_filtered_titles.txt"),
                "Rows": nrows,
                "Status": "ok"
            })
        except Exception as e:
            rows.append({
                "PDF": str(pdf),
                "OutDir": str(per_pdf_out),
                "FullSchema": "",
                "Baseline": "",
                "Filtered": "",
                "Rows": "",
                "Status": f"error: {e.__class__.__name__}: {e}"
            })

    # Write index CSV
    index_csv = out_root / "batch_index.csv"
    import csv
    with index_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["PDF","OutDir","FullSchema","Baseline","Filtered","Rows","Status"])
        w.writeheader()
        w.writerows(rows)
    print(f"[batch-build] Wrote index: {index_csv} ({len(rows)} files)")
    return index_csv


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="PW builder + comparator (deterministic).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # ---- Build ----
    b = sub.add_parser("build", help="Parse productions -> standardized CSV + baseline/filtered lists")
    b.add_argument("input", help="Per-PDF folder (with productions/), a productions/ folder, or a structured .txt")
    b.add_argument("-o","--outdir", required=True, help="Output directory for CSV + lists")
    b.add_argument("--label", required=True, help='Label for this report (e.g., "PW Aug 21")')

    # ---- Compare (now folder-based) ----
    c = sub.add_parser("compare", help="Compare two run folders")
    c.add_argument("--old-dir", required=True, help="Folder containing *_FullSchema.csv and *_baseline_titles.txt")
    c.add_argument("--new-dir", required=True, help="Folder containing *_FullSchema.csv and *_baseline_titles.txt")
    c.add_argument("-o","--outdir", required=True, help="Output directory for comparison results")
    c.add_argument("--latest-date", required=True, help='For filename, e.g., "Sep_04_2025"')

    # ---- Master Compare (now folder-based for weekly) ----
    m = sub.add_parser("master-compare", help="Compare a weekly run folder directly to a region-specific master CSV")
    m.add_argument("--master-csv", required=True, help="Your master spreadsheet CSV")
    m.add_argument("--weekly-dir", required=True, help="Weekly run folder containing *_FullSchema.csv and *_baseline_titles.txt")
    m.add_argument("-o","--outdir", required=True, help="Output directory for results")
    m.add_argument("--latest-date", required=True, help='For filename, e.g., "Sep_04_2025"')
    m.add_argument("--region", help="Optional region bucket filter/override")

    # ---- Batch Build (folder of PDFs) ----
    bb = sub.add_parser("batch-build", help="Process all PDFs in a folder → per-PDF builds")
    bb.add_argument("--input-dir", required=True, help="Folder containing PW PDFs")
    bb.add_argument("-o","--outdir", required=True, help="Output root folder for per-PDF results")
    bb.add_argument("--glob", default="*.pdf", help="Glob pattern (default: *.pdf)")
    bb.add_argument("--no-skip-clean", action="store_true", help="Force re-clean even if cleaned exists")
    bb.add_argument("--no-resume", action="store_true", help="Rebuild even if FullSchema already exists")


    args = ap.parse_args()

    if args.cmd == "build":
        build_cmd(Path(args.input), Path(args.outdir), args.label)

    elif args.cmd == "compare":
        old_csv, old_baseline, old_label = resolve_run_folder(Path(args.old_dir))
        new_csv, new_baseline, new_label = resolve_run_folder(Path(args.new_dir))
        compare_cmd(
            old_csv, new_csv,
            old_baseline, new_baseline,
            old_label, new_label,
            Path(args.outdir), args.latest_date
        )

    elif args.cmd == "master-compare":
        weekly_csv, weekly_baseline, weekly_label = resolve_run_folder(Path(args.weekly_dir))
        master_compare_cmd(
            Path(args.master_csv),
            weekly_csv, weekly_baseline, weekly_label,
            Path(args.outdir), args.latest_date,
            region=(args.region or "")
        )
    elif args.cmd == "batch-build":
        index = batch_build_cmd(
            Path(args.input_dir),
            Path(args.outdir),
            glob_pattern=args.glob,
            skip_clean=(not args.no_skip_clean),
            resume=(not args.no_resume)
    )
    

    else:
        ap.print_help()


# ===== GUI =====

import tkinter as tk
from tkinter import filedialog, ttk, messagebox
from pathlib import Path
import pandas as pd
from PIL import Image, ImageTk

# ----- BRAND COLORS -----
HORIZON_BLUE    = "#0A55A4"  # Primary accent
VERDANT_GLOW    = "#18A94F"
LIGHT_SCARLET   = "#ED1B2C"
ALMOST_MIDNIGHT = "#0B0A0A"
DARK_SLATE      = "#343434"
PURE_WHITE      = "#FFFFFF"

# NEW: mid grey between black and slate for preview rows
MID_GREY        = "#262626"

WINDOW_BG   = DARK_SLATE
CARD_BG     = ALMOST_MIDNIGHT
ACCENT      = HORIZON_BLUE
ACCENT_HOVER = "#1568C4"
TEXT_MAIN   = PURE_WHITE
TEXT_MUTED  = "#D0D0D0"
BORDER_DARK = "#202020"

# use mid grey as the alternate row color
TABLE_STRIPE = MID_GREY



class ProductionWeeklyGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("IP Production Weekly Extractor")
        root.configure(bg=WINDOW_BG)

        self.logo_img = None

        # ---- ttk style ----
        style = ttk.Style(root)
        style.theme_use("clam")

        # Frames
        style.configure("Dark.TFrame", background=WINDOW_BG)
        style.configure("Card.TFrame", background=CARD_BG)  # used mainly for preview

        # Labels
        style.configure("Title.TLabel",
                        background=WINDOW_BG,
                        foreground=TEXT_MAIN,
                        font=("Poppins Black", 18))
        style.configure("Subtitle.TLabel",
                        background=WINDOW_BG,
                        foreground=TEXT_MUTED,
                        font=("Space Grotesk", 10))
        style.configure("Field.TLabel",
                        background=WINDOW_BG,
                        foreground=TEXT_MAIN,
                        font=("Space Grotesk", 10, "bold"))
        style.configure("Hint.TLabel",
                        background=WINDOW_BG,
                        foreground=TEXT_MUTED,
                        font=("Space Grotesk", 9))
        style.configure("Status.TLabel",
                        background=WINDOW_BG,
                        foreground=TEXT_MUTED,
                        font=("Space Grotesk", 9))
        style.configure("Logo.TLabel",
                        background=WINDOW_BG)

        # Entries (inputs)
        style.configure(
            "IP.TEntry",
            fieldbackground=WINDOW_BG,
            foreground=TEXT_MAIN,
            bordercolor="#A0A0A0",
            lightcolor="#A0A0A0",
            darkcolor="#A0A0A0",
            insertcolor=TEXT_MAIN,
            borderwidth=1,
            relief="flat",
            padding=3,
        )

        # Notebook
        style.configure("IP.TNotebook",
                        background=WINDOW_BG,
                        borderwidth=0)
        style.configure("IP.TNotebook.Tab",
                        background=WINDOW_BG,
                        foreground=TEXT_MUTED,
                        padding=(12, 4),
                        font=("Space Grotesk", 9, "bold"))
        style.map("IP.TNotebook.Tab",
                  background=[("selected", WINDOW_BG)],
                  foreground=[("selected", TEXT_MAIN)])

        # Buttons – all blue
        style.configure("Primary.TButton",
                        background=ACCENT,
                        foreground=PURE_WHITE,
                        borderwidth=0,
                        padding=(18, 7),
                        focusthickness=0,
                        font=("Space Grotesk SemiBold", 10))
        style.map("Primary.TButton",
                  background=[("active", ACCENT_HOVER)])

        style.configure("Secondary.TButton",
                        background=ACCENT,
                        foreground=PURE_WHITE,
                        borderwidth=0,
                        padding=(14, 6),
                        focusthickness=0,
                        font=("Space Grotesk", 9))
        style.map("Secondary.TButton",
                  background=[("active", ACCENT_HOVER)])

        # Treeview (preview table)
        style.configure("Dark.Treeview",
                        background=CARD_BG,
                        fieldbackground=CARD_BG,
                        foreground=TEXT_MAIN,
                        bordercolor=BORDER_DARK,
                        rowheight=40,
                        font=("Space Grotesk", 9))
        style.configure("Dark.Treeview.Heading",
                        background="#3C3C3C",
                        foreground=TEXT_MAIN,
                        bordercolor=BORDER_DARK,
                        font=("Space Grotesk SemiBold", 9))
        style.map("Dark.Treeview",
                  background=[("selected", "#25507B")],
                  foreground=[("selected", TEXT_MAIN)])

        # Scrollbars
        style.configure("Dark.Vertical.TScrollbar",
                        background=ACCENT,
                        troughcolor=MID_GREY,
                        bordercolor=MID_GREY,
                        arrowcolor=PURE_WHITE)

        style.configure("Dark.Horizontal.TScrollbar",
                        background=ACCENT,
                        troughcolor=MID_GREY,
                        bordercolor=MID_GREY,
                        arrowcolor=PURE_WHITE)


        # Progress bar
        style.configure("IP.Horizontal.TProgressbar",
                        background=ACCENT,
                        troughcolor=BORDER_DARK,
                        bordercolor=BORDER_DARK,
                        thickness=4)

        # ---- Header with logo ----
        header = ttk.Frame(root, style="Dark.TFrame")
        header.pack(fill="x", padx=16, pady=(4, 2))
        self._load_logo(header)

        # ---- Main area (no black box: same bg as window) ----
        card = ttk.Frame(root, style="Dark.TFrame")
        card.pack(fill="both", expand=True, padx=16, pady=(0, 8))

        self.notebook = ttk.Notebook(card, style="IP.TNotebook")
        self.notebook.pack(fill="both", expand=True)

        # Preview area uses darker card bg
        self.frame_preview = ttk.Frame(root, style="Card.TFrame")
        self.frame_preview.pack(fill="both", expand=True, padx=16, pady=(0, 0))

        # Status bar
        status_frame = ttk.Frame(root, style="Dark.TFrame")
        status_frame.pack(fill="x", padx=16, pady=(0, 10))

        self.status_var = tk.StringVar(value="Status: Ready")
        ttk.Label(status_frame,
                  textvariable=self.status_var,
                  style="Status.TLabel").pack(side="left")

        self.progress = ttk.Progressbar(status_frame,
                                        mode="determinate",
                                        style="IP.Horizontal.TProgressbar",
                                        length=200)
        self.progress.pack(side="right")

        # Tabs
        self._build_tab()
        self._compare_tab()
        self._master_tab()
        self._toolbar()

    # ---------- Logo loader ----------
    def _load_logo(self, parent: ttk.Frame):
        """
        Load PW_Extractor_Long-01-01.png, center it, and offset slightly left.
        Header height is driven by the logo height so it doesn't sit under the box.
        """
        try:
            script_dir = Path(__file__).resolve().parent
        except NameError:
            script_dir = Path(".").resolve()
        logo_path = script_dir / "PW_Extractor_Long-01-01.png"

        if logo_path.exists():
            try:
                img = Image.open(logo_path)
                max_w = 420
                if img.width > max_w:
                    ratio = max_w / img.width
                    img = img.resize(
                        (int(img.width * ratio), int(img.height * ratio)),
                        Image.LANCZOS
                    )

                self.logo_img = ImageTk.PhotoImage(img)

                label = ttk.Label(
                    parent,
                    image=self.logo_img,
                    style="Logo.TLabel"
                )
                # Centered, nudged left by giving more padding on the right
                label.pack(expand=True, pady=(4, 4), padx=(0, 50))

                # Now that we know the rendered height, lock the header to it
                parent.update_idletasks()
                header_h = self.logo_img.height() + 8  # a little breathing room
                parent.configure(height=header_h)
                parent.pack_propagate(False)
                return
            except Exception:
                pass

        # Fallback text banner (still centered)
        parent.pack_propagate(True)
        box = ttk.Frame(parent, style="Dark.TFrame")
        box.pack(expand=True)
        ttk.Label(box, text="INDUSTRIAL PIXEL",
                style="Title.TLabel").grid(row=0, column=0, sticky="n")
        ttk.Label(box,
                text="3D STUDIOS · PRODUCTION WEEKLY EXTRACTOR",
                style="Subtitle.TLabel").grid(row=1, column=0, sticky="n", pady=(2, 0))

        # ---------- small helpers shared by the GUI ----------

    def _set_status(self, text: str, busy: bool = False):
        """
        Update the status bar text and optionally spin the progress bar.
        Safe if called before widgets exist.
        """
        if hasattr(self, "status_var"):
            self.status_var.set(f"Status: {text}")

        if hasattr(self, "progress"):
            if busy:
                self.progress.start(10)
            else:
                self.progress.stop()

    def _browse_file(self, entry_widget, filetypes):
        """
        Open a file picker and drop the selected path into the given Entry.
        """
        path = filedialog.askopenfilename(filetypes=filetypes)
        if path:
            entry_widget.delete(0, tk.END)
            entry_widget.insert(0, path)

    def _browse_folder(self, entry_widget):
        """
        Open a folder picker and drop the selected path into the given Entry.
        """
        path = filedialog.askdirectory()
        if path:
            entry_widget.delete(0, tk.END)
            entry_widget.insert(0, path)

    def _clear_preview(self):
        """
        Remove any existing widgets from the preview frame.
        """
        for w in getattr(self, "frame_preview", []).winfo_children():
            w.destroy()


        # ---------- helpers ----------
    def _show_preview(self, csv_path: str, wrap_text: bool = True, max_rows: int = 200):
        """
        Show a CSV in a fixed-height Treeview that:
        - uses mid-grey striping
        - keeps columns independent when resized
        - scrolls faster
        - stays within the window (scroll instead of window blow-up)
        """
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig").fillna("")
        except Exception as e:
            messagebox.showerror("Error", f"Could not read CSV:\n{e}")
            return

        if len(df) > max_rows:
            df = df.head(max_rows)

        def _wrap(s: str, width: int = 80) -> str:
            if not wrap_text or not isinstance(s, str) or len(s) <= width:
                return s
            out, line, count = [], [], 0
            for ch in s:
                line.append(ch)
                count += 1
                if count >= width and ch == " ":
                    out.append("".join(line))
                    line, count = [], 0
            if line:
                out.append("".join(line))
            return "\n".join(out)

        # Clear any previous preview
        self._clear_preview()

        vsb = ttk.Scrollbar(
            self.frame_preview,
            orient="vertical",
            style="Dark.Vertical.TScrollbar",
        )
        hsb = ttk.Scrollbar(
            self.frame_preview,
            orient="horizontal",
            style="Dark.Horizontal.TScrollbar",
        )

        tree = ttk.Treeview(
            self.frame_preview,
            show="headings",
            xscrollcommand=hsb.set,
            yscrollcommand=vsb.set,
            style="Dark.Treeview",
            height=18,          # fixed visible rows; rest scrolls
        )

        vsb.config(command=tree.yview)
        hsb.config(command=tree.xview)

        cols = list(df.columns)
        tree["columns"] = cols

        # Keep each column independent (no global stretch)
        for col in cols:
            tree.heading(col, text=col, anchor="w")

            col_lower = col.lower()
            if "description" in col_lower or "all locations" in col_lower or "notes" in col_lower:
                width = 420
            else:
                width = 180

            tree.column(col, width=width, stretch=False, anchor="w")

        # Populate rows with simple wrapping
        for idx, (_, row) in enumerate(df.iterrows()):
            values = [_wrap(str(row[col])) for col in cols]
            tag = "odd" if idx % 2 else "even"
            tree.insert("", "end", values=values, tags=(tag,))

        # Stripe rows
        tree.tag_configure("odd", background=CARD_BG)
        tree.tag_configure("even", background=TABLE_STRIPE)

        # Layout
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        self.frame_preview.rowconfigure(0, weight=1)
        self.frame_preview.columnconfigure(0, weight=1)
        self.frame_preview.update_idletasks()

        # Double-click → open full text popup for that cell
        def _on_double_click(event):
            region = tree.identify("region", event.x, event.y)
            if region != "cell":
                return

            row_id = tree.identify_row(event.y)
            col_id = tree.identify_column(event.x)
            if not row_id or not col_id:
                return

            col_index = int(col_id.replace("#", "")) - 1
            if col_index < 0 or col_index >= len(cols):
                return

            col_name = cols[col_index]
            cell_value = tree.set(row_id, col_name)
            self._open_cell_details(cell_value, title=col_name)

        tree.bind("<Double-1>", _on_double_click)

        # Faster mousewheel scrolling (including Shift+wheel horizontal)
        self._attach_tree_scroll(tree)

        # Clamp the main window so it doesn't span multiple displays
        try:
            self.root.update_idletasks()

            screen_w = self.root.winfo_screenwidth()
            screen_h = self.root.winfo_screenheight()

            max_w = min(screen_w, 1680)
            max_h = min(screen_h, 880)

            cur_w = self.root.winfo_width()
            cur_h = self.root.winfo_height()

            new_w = min(cur_w, max_w)
            new_h = min(cur_h, max_h)

            self.root.geometry(f"{new_w}x{new_h}")
        except Exception:
            pass

        # Double-click to show full cell contents in a popup
    def _on_double_click(event):
        region = tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        row_id = tree.identify_row(event.y)
        col_id = tree.identify_column(event.x)
        if not row_id or not col_id:
            return

        col_index = int(col_id.replace("#", "")) - 1
        if col_index < 0 or col_index >= len(cols):
            return

        col_name = cols[col_index]
        cell_value = tree.set(row_id, col_name)
        self._open_cell_details(cell_value, title=col_name)

        tree.bind("<Double-1>", _on_double_click)


        # Faster mousewheel scrolling
        self._attach_tree_scroll(tree)
        
    def _open_cell_details(self, value: str, title: str = "Cell details"):
        """
        Show the full text of a cell in a small popup window with word wrapping.
        """
        if value is None:
            value = ""

        win = tk.Toplevel(self.root)
        win.title(title)
        win.configure(bg=WINDOW_BG)

        # Size it reasonably
        win.geometry("600x300")

        lbl = tk.Label(
            win,
            text=title,
            bg=WINDOW_BG,
            fg=TEXT_MAIN,
            font=("Space Grotesk", 10, "bold"),
            anchor="w"
        )
        lbl.pack(fill="x", padx=10, pady=(10, 4))

        # Read-only text widget with wrapping
        txt = tk.Text(
            win,
            wrap="word",
            bg=CARD_BG,
            fg=TEXT_MAIN,
            insertbackground=TEXT_MAIN,
            relief="flat",
            padx=8,
            pady=8
        )
        txt.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        txt.insert("1.0", str(value))
        txt.config(state="disabled")

        # Close on Escape
        win.bind("<Escape>", lambda e: win.destroy())


        # Clamp window size so it never extends past the display
        try:
            screen_w = self.root.winfo_screenwidth()
            screen_h = self.root.winfo_screenheight()
            margin = 80
            current_w = self.root.winfo_width()
            current_h = self.root.winfo_height()
            new_w = min(current_w, screen_w - margin)
            new_h = min(current_h, screen_h - margin)
            self.root.geometry(f"{new_w}x{new_h}")
        except Exception:
            pass


    def _attach_tree_scroll(self, tree: ttk.Treeview):
        """
        Fast scrolling:
          - MouseWheel = vertical scroll
          - Shift + MouseWheel = horizontal scroll
        """
        def _on_mousewheel(event):
            # Shift pressed? scroll horizontally
            # On Windows, Shift sets bit 0x0001 in state
            if event.state & 0x0001:
                delta = int(-event.delta / 5)
                tree.xview_scroll(delta, "units")
            else:
                delta = int(-event.delta / 40)
                tree.yview_scroll(delta, "units")
            return "break"

        # Windows / most platforms
        tree.bind("<MouseWheel>", _on_mousewheel)

        # macOS / some X11 variants use different event names;
        # binding again here won't hurt on Windows.
        tree.bind("<Shift-MouseWheel>", _on_mousewheel)


    def run_build(self):
        try:
            self._set_status("Running build…", busy=True)

            input_path = Path(self.entry_build_input.get().strip())
            outdir = Path(self.entry_build_outdir.get().strip() or "")
            label = self.entry_build_label.get().strip()

            if not input_path.exists():
                self._set_status("Error")
                messagebox.showerror("Error", f"Input not found: {input_path}")
                return

            if input_path.suffix.lower() == ".pdf":
                out_root = outdir if str(outdir) else (input_path.parent / "pw_output")
                pdf_outdir = out_root / input_path.stem
                pdf_outdir.mkdir(parents=True, exist_ok=True)

                cleaned = pdf_outdir / f"{input_path.stem}_cleaned.pdf"
                out_struct = pdf_outdir / f"{input_path.stem}_cleaned.structured.txt"
                per_page_dir = pdf_outdir / "pages"
                per_prod_dir = pdf_outdir / "productions"

                clean_pdf(input_path, cleaned, strip=100,
                          units="pixels", dpi=72.0, mode="redact")
                extract_text_structured(cleaned, out_struct, per_page_dir, per_prod_dir)

                input_for_build = pdf_outdir
                if not label:
                    label = input_path.stem

            elif input_path.is_dir():
                input_for_build = input_path
                if not label:
                    label = input_path.name
            else:
                self._set_status("Error")
                messagebox.showerror("Error", f"Unsupported input: {input_path}")
                return

            csv_path, baseline, filtered = build_cmd(input_for_build, outdir, label)

            try:
                fullschema = max(Path(outdir).glob("*_FullSchema.csv"),
                                 key=lambda p: p.stat().st_mtime)
                preview_target = fullschema
            except ValueError:
                preview_target = Path(csv_path)

            messagebox.showinfo("Build Complete", f"Wrote CSV:\n{csv_path}")
            self._show_preview(str(preview_target))
            self._set_status("Ready")
        except Exception as e:
            self._set_status("Error")
            messagebox.showerror("Error", f"{e.__class__.__name__}: {e}")

    # ---------- Build tab ----------
    def _build_tab(self):
        """
        Build tab: input PDF/folder + output folder + label.
        Uses the dark/slate styling + blue buttons and styled entries.
        """
        frame = ttk.Frame(self.notebook, style="Dark.TFrame")
        self.notebook.add(frame, text="Build")

        r = 0
        ttk.Label(
            frame,
            text="Input (PDF):",
            style="Field.TLabel",
        ).grid(row=r, column=0, sticky="e", padx=8, pady=6)


        self.entry_build_input = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_build_input.grid(row=r, column=1, sticky="we", pady=6)

        ttk.Button(
            frame,
            text="Browse",
            style="Secondary.TButton",
            command=lambda: self._browse_file(
                self.entry_build_input,
                [("PDF", "*.pdf"), ("Text", "*.txt"), ("All", "*.*")]
            ),
        ).grid(row=r, column=2, padx=(8, 0), pady=6)
        r += 1

        ttk.Label(
            frame,
            text="Output Folder:",
            style="Field.TLabel",
        ).grid(row=r, column=0, sticky="e", padx=8, pady=6)

        self.entry_build_outdir = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_build_outdir.grid(row=r, column=1, sticky="we", pady=6)

        ttk.Button(
            frame,
            text="Browse",
            style="Secondary.TButton",
            command=lambda: self._browse_folder(self.entry_build_outdir),
        ).grid(row=r, column=2, padx=(8, 0), pady=6)
        r += 1

        ttk.Label(
            frame,
            text="Label:",
            style="Field.TLabel",
        ).grid(row=r, column=0, sticky="e", padx=8, pady=6)

        self.entry_build_label = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_build_label.grid(row=r, column=1, sticky="we", pady=6)
        r += 1

        ttk.Button(
            frame,
            text="Run Build",
            style="Primary.TButton",
            command=self.run_build,
        ).grid(row=r, column=1, pady=(8, 12))

        frame.columnconfigure(1, weight=1)

    # ---------- Compare tab ----------
    def _compare_tab(self):
        frame = ttk.Frame(self.notebook, style="Dark.TFrame")
        self.notebook.add(frame, text="Compare (Old vs New)")

        ttk.Label(frame, text="Old Run Folder:",
                  style="Field.TLabel").grid(row=0, column=0, sticky="e", padx=8, pady=6)
        self.entry_old_dir = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_old_dir.grid(row=0, column=1, sticky="we", pady=6)
        ttk.Button(frame, text="Browse",
                   style="Secondary.TButton",
                   command=lambda: self._browse_folder(self.entry_old_dir)
                   ).grid(row=0, column=2, padx=(8, 0), pady=6)

        ttk.Label(frame, text="New Run Folder:",
                  style="Field.TLabel").grid(row=1, column=0, sticky="e", padx=8, pady=6)
        self.entry_new_dir = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_new_dir.grid(row=1, column=1, sticky="we", pady=6)
        ttk.Button(frame, text="Browse",
                   style="Secondary.TButton",
                   command=lambda: self._browse_folder(self.entry_new_dir)
                   ).grid(row=1, column=2, padx=(8, 0), pady=6)

        ttk.Label(frame, text="Output Folder:",
                  style="Field.TLabel").grid(row=2, column=0, sticky="e", padx=8, pady=6)
        self.entry_compare_outdir = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_compare_outdir.grid(row=2, column=1, sticky="we", pady=6)
        ttk.Button(frame, text="Browse",
                   style="Secondary.TButton",
                   command=lambda: self._browse_folder(self.entry_compare_outdir)
                   ).grid(row=2, column=2, padx=(8, 0), pady=6)

        ttk.Label(frame, text="Latest Date (e.g. Sep_04_2025):",
                  style="Field.TLabel").grid(row=3, column=0, sticky="e", padx=8, pady=6)
        self.entry_compare_date = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_compare_date.grid(row=3, column=1, sticky="we", pady=6)

        ttk.Button(frame, text="Run Compare",
                   style="Primary.TButton",
                   command=self.run_compare).grid(row=4, column=1, pady=(8, 12))

        frame.columnconfigure(1, weight=1)

    def run_compare(self):
        try:
            self._set_status("Running compare…", busy=True)

            old_dir = Path(self.entry_old_dir.get().strip())
            new_dir = Path(self.entry_new_dir.get().strip())
            outdir = Path(self.entry_compare_outdir.get().strip())

            if not old_dir.exists():
                self._set_status("Error")
                messagebox.showerror("Error", f"Old run folder not found: {old_dir}")
                return
            if not new_dir.exists():
                self._set_status("Error")
                messagebox.showerror("Error", f"New run folder not found: {new_dir}")
                return

            old_csv, old_baseline, old_label = resolve_run_folder(old_dir)
            new_csv, new_baseline, new_label = resolve_run_folder(new_dir)

            from datetime import datetime
            latest_date = self.entry_compare_date.get().strip() or \
                datetime.now().strftime("%b_%d_%Y")

            compare_cmd(
                Path(old_csv), Path(new_csv),
                Path(old_baseline), Path(new_baseline),
                old_label, new_label,
                Path(outdir), latest_date
            )

            out_file = outdir / f"PW_{latest_date}_Comparison_FullSchema.csv"
            if out_file.exists():
                self._show_preview(str(out_file))
            else:
                messagebox.showinfo("Done",
                                    f"Compare finished but output file not found:\n{out_file}")
            self._set_status("Ready")
        except Exception as e:
            self._set_status("Error")
            messagebox.showerror("Compare Error", f"{e.__class__.__name__}: {e}")

    # ---------- Master Compare tab ----------
    def _master_tab(self):
        frame = ttk.Frame(self.notebook, style="Dark.TFrame")
        self.notebook.add(frame, text="Master Compare")

        r = 0
        ttk.Label(frame, text="Master Folder:",
                  style="Field.TLabel").grid(row=r, column=0, sticky="e", padx=8, pady=6)
        self.entry_master_dir = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_master_dir.grid(row=r, column=1, sticky="we", pady=6)
        ttk.Button(frame, text="Browse",
                   style="Secondary.TButton",
                   command=lambda: self._browse_folder(self.entry_master_dir)
                   ).grid(row=r, column=2, padx=(8, 0), pady=6)
        r += 1

        ttk.Label(frame, text="Weekly Run Folder:",
                  style="Field.TLabel").grid(row=r, column=0, sticky="e", padx=8, pady=6)
        self.entry_weekly_dir = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_weekly_dir.grid(row=r, column=1, sticky="we", pady=6)
        ttk.Button(frame, text="Browse",
                   style="Secondary.TButton",
                   command=lambda: self._browse_folder(self.entry_weekly_dir)
                   ).grid(row=r, column=2, padx=(8, 0), pady=6)
        r += 1

        ttk.Label(frame, text="Output Folder:",
                  style="Field.TLabel").grid(row=r, column=0, sticky="e", padx=8, pady=6)
        self.entry_master_outdir = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_master_outdir.grid(row=r, column=1, sticky="we", pady=6)
        ttk.Button(frame, text="Browse",
                   style="Secondary.TButton",
                   command=lambda: self._browse_folder(self.entry_master_outdir)
                   ).grid(row=r, column=2, padx=(8, 0), pady=6)
        r += 1

        ttk.Label(frame, text="Latest Date (e.g. Sep_11_2025):",
                  style="Field.TLabel").grid(row=r, column=0, sticky="e", padx=8, pady=6)
        self.entry_master_date = ttk.Entry(frame, width=60, style="IP.TEntry")
        self.entry_master_date.grid(row=r, column=1, sticky="we", pady=6)
        r += 1

        ttk.Label(frame, text="Region:",
                  style="Field.TLabel").grid(row=r, column=0, sticky="e", padx=8, pady=6)
        self.combo_region = ttk.Combobox(
            frame,
            width=57,
            state="readonly",
            values=[
                "All Regions (batch)",
                "United States",
                "West Coast Canada",
                "East Coast Canada",
                "Quebec",
                "Ireland/Hungary",
                "Australia/New Zealand",
                "Europe/Other",
                "Other",
            ],
        )
        self.combo_region.grid(row=r, column=1, sticky="we", pady=6)
        self.combo_region.set("All Regions (batch)")

               # Move to next row for buttons
        r += 1

        # Run Master Compare button
        btn_run = ttk.Button(
            frame,
            text="Run Master Compare",
            style="Primary.TButton",
            command=self.run_master_compare,
        )
        btn_run.grid(row=r, column=1, sticky="w", padx=(0, 8), pady=(8, 12))

        # Preview Existing CSV button RIGHT NEXT TO Run Master Compare
        btn_prev = ttk.Button(
            frame,
            text="Preview Existing CSV",
            style="Secondary.TButton",
            command=self.preview_master_compare_csv,
        )
        btn_prev.grid(row=r, column=2, sticky="w", padx=(0,0), pady=(8, 12))

    def run_master_compare(self):
        try:
            self._set_status("Running master compare…", busy=True)

            master_dir = Path(self.entry_master_dir.get().strip())
            weekly_dir = Path(self.entry_weekly_dir.get().strip())
            outdir = Path(self.entry_master_outdir.get().strip())
            latest_date = self.entry_master_date.get().strip()
            region = (self.combo_region.get() or "").strip()

            if not master_dir.exists():
                self._set_status("Error")
                messagebox.showerror("Error", f"Master folder not found: {master_dir}")
                return
            if not weekly_dir.exists():
                self._set_status("Error")
                messagebox.showerror("Error", f"Weekly run folder not found: {weekly_dir}")
                return
            if not latest_date:
                self._set_status("Error")
                messagebox.showerror(
                    "Error",
                    "Please enter Latest Date (e.g., Sep_11_2025).",
                )
                return

            weekly_csv, weekly_baseline, weekly_label = resolve_run_folder(weekly_dir)

            # --- Batch: run all regions and write ONE combined summary ---
            if region == "All Regions (batch)":
                summary_acc: dict = {}
                for reg in REGION_FILE_MAP.keys():
                    master_compare_cmd(
                        Path(master_dir),
                        Path(weekly_csv), Path(weekly_baseline), weekly_label,
                        Path(outdir), latest_date,
                        region=reg,
                        summary_acc=summary_acc,
                    )

                total_pw = summary_acc.get("total_pw", 0)
                pushed_total = summary_acc.get("pushed_total", 0)
                summary_txt = outdir / f"PW_{latest_date}_SUMMARY.txt"

                lines = [
                    f"Production Weekly Summary — {weekly_label} (ALL REGIONS)",
                    "",
                    f"Total productions this issue (including filtered): {total_pw}",
                    f"Productions with DATE PUSHED BACK (all regions): {pushed_total}",
                    "",
                    "Files:",
                ]
                for f_info in summary_acc.get("files", []):
                    lines.append(f"- {f_info['region']}: {f_info['master_compare']}")

                summary_txt.write_text("\n".join(lines), encoding="utf-8")
                print(f"[summary] Wrote combined summary: {summary_txt}")

                messagebox.showinfo(
                    "Master Compare Complete",
                    f"Master compare finished for all regions.\n\n"
                    f"Summary:\n{summary_txt}\n\n"
                    f"Output folder:\n{outdir}",
                )

            # --- Single region: per-region summary + preview ---
            else:
                master_compare_cmd(
                    Path(master_dir),
                    Path(weekly_csv), Path(weekly_baseline), weekly_label,
                    Path(outdir), latest_date,
                    region=region or "",
                )

                safe_region = (region or "All")
                for bad in ("/", "\\", ":", "*", "?", "\"", "<", ">", "|"):
                    safe_region = safe_region.replace(bad, "_")
                safe_region = safe_region.replace(" ", "_")

                out_file = outdir / f"PW_{latest_date}_VS_MASTER_{safe_region}.csv"
                if out_file.exists():
                    self._show_preview(str(out_file))
                else:
                    messagebox.showinfo(
                        "Done",
                        f"Master compare finished but output file not found:\n{out_file}",
                    )

            self._set_status("Ready")

        except Exception as e:
            self._set_status("Error")
            messagebox.showerror(
                "Master Compare Error",
                f"{e.__class__.__name__}: {e}",
            )


        
    def preview_master_compare_csv(self):
        """Open an already-built VS_MASTER CSV for the selected region/date
        without re-running the compare."""
        try:
            outdir = Path(self.entry_master_outdir.get().strip())
            latest_date = self.entry_master_date.get().strip()
            region = (self.combo_region.get() or "").strip()

            if not outdir.exists():
                messagebox.showerror(
                    "Error",
                    f"Output folder not found:\n{outdir}",
                )
                return
            if not latest_date:
                messagebox.showerror(
                    "Error",
                    "Please enter Latest Date (e.g., Sep_11_2025).",
                )
                return
            if not region:
                messagebox.showerror(
                    "Error",
                    "Please select a Region.",
                )
                return

            # Build the same safe region suffix used in run_master_compare
            safe_region = (region or "All")
            for bad in ("/", "\\", ":", "*", "?", "\"", "<", ">", "|"):
                safe_region = safe_region.replace(bad, "_")
            safe_region = safe_region.replace(" ", "_")

            csv_path = outdir / f"PW_{latest_date}_VS_MASTER_{safe_region}.csv"

            if not csv_path.exists():
                messagebox.showerror(
                    "Not Found",
                    "Could not find a VS_MASTER CSV for this date/region:\n"
                    f"{csv_path}\n\n"
                    "Make sure you've already run Master Compare for this region.",
                )
                return

            # Uses the existing CSV preview UI
            self._show_preview(str(csv_path))

        except Exception as e:
            messagebox.showerror(
                "Preview Error",
                f"{e.__class__.__name__}: {e}",
            )


    # ---------- Toolbar ----------
    def _toolbar(self):
        bar = ttk.Frame(self.root, style="Dark.TFrame")
        bar.pack(fill="x", side="top", padx=16, pady=(0, 0))

        ttk.Button(bar,
                   text="Batch Build (Folder)",
                   style="Secondary.TButton",
                   command=self.run_batch_build).pack(side="left", padx=(0, 0), pady=4)

    def run_batch_build(self):
        try:
            self._set_status("Batch build running…", busy=True)

            in_dir_str = filedialog.askdirectory(title="Select folder with PW PDFs")
            if not in_dir_str:
                self._set_status("Ready")
                return
            in_dir = Path(in_dir_str)

            out_root_str = filedialog.askdirectory(
                title="Select output root folder")
            if not out_root_str:
                self._set_status("Ready")
                return
            out_root = Path(out_root_str)

            import tkinter.simpledialog as sd
            glob_pat = sd.askstring("Glob pattern",
                                    "File pattern:",
                                    initialvalue="*.pdf") or "*.pdf"

            index_csv = batch_build_cmd(in_dir, out_root,
                                        glob_pattern=glob_pat,
                                        skip_clean=True, resume=True)
            messagebox.showinfo("Batch Build Complete",
                                f"Index written:\n{index_csv}")

            hits = sorted(out_root.rglob("*_FullSchema.csv"),
                          key=lambda p: p.stat().st_mtime,
                          reverse=True)
            if hits:
                self._show_preview(str(hits[0]))
            self._set_status("Ready")
        except Exception as e:
            self._set_status("Error")
            messagebox.showerror("Batch Build Error", f"{e.__class__.__name__}: {e}")


def launch_gui():
    root = tk.Tk()
    ProductionWeeklyGUI(root)
    root.mainloop()


if __name__ == "__main__":
    launch_gui()

