#!/usr/bin/env python3
# remove_bottom_watermark.py
# Usage:
#   python remove_bottom_watermark.py in.pdf [out.pdf] [--strip 100 --units pixels --dpi 72 --mode redact]
# If out.pdf is omitted, script will create "<input>_cleaned.pdf".

import sys
import argparse
from pathlib import Path
import tempfile
import shutil
import fitz  # PyMuPDF


def px_to_points(px: float, dpi: float) -> float:
    return (px / dpi) * 72.0


def _preflight_pdf(p: Path):
    """Fast checks to avoid trashing invalid inputs."""
    if not p.exists():
        raise FileNotFoundError(f"Input PDF not found: {p}")
    if p.stat().st_size == 0:
        raise ValueError(f"Input file is empty (0 bytes): {p}")
    head = p.read_bytes()[:2048]
    lower = head.lower()
    if b"<html" in lower or b"<!doctype html" in lower:
        raise ValueError(
            f"Not a PDF: looks like an HTML page saved as .pdf: {p}\n"
            f"Tip: download the actual PDF (right-click → Save link as…) or open in Acrobat and Save As."
        )
    # Allow %PDF- anywhere in the first KB (some generators add a few bytes first)
    if b"%PDF-" not in head[:1024]:
        raise ValueError(f"Not a valid PDF (missing %PDF- header in first 1KB): {p}")


def _open_with_fallback(p: Path):
    """Open via path, falling back to stream for odd path/encoding cases."""
    try:
        return fitz.open(p)
    except Exception:
        buf = p.read_bytes()
        return fitz.open(stream=buf, filetype="pdf")


def process_pdf(in_path: Path, out_path: Path, strip: float, units: str, dpi: float, mode: str):
    import os, time

    in_path = Path(in_path).resolve()
    out_path = Path(out_path).resolve()

    # Never allow in-place overwrite
    if in_path == out_path:
        raise ValueError(f"Refusing to overwrite source: {in_path}")

    # Preflight input
    _preflight_pdf(in_path)

    # Units
    strip_pt = px_to_points(strip, dpi) if units == "pixels" else strip
    if strip_pt <= 0:
        raise ValueError("--strip must be > 0")

    # Temp file in the SAME DIRECTORY as the destination to avoid cross-drive moves
    tmp_out = out_path.with_name(f"{out_path.stem}.tmp-{os.getpid()}{out_path.suffix}")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Open with fallback and process
    doc = _open_with_fallback(in_path)
    try:
        for page in doc:
            rect = page.rect
            bottom_strip = fitz.Rect(rect.x0, rect.y1 - strip_pt, rect.x1, rect.y1)
            if mode == "crop":
                new_rect = fitz.Rect(rect.x0, rect.y0, rect.x1, max(rect.y0, rect.y1 - strip_pt))
                if new_rect.height <= 0:
                    new_rect = fitz.Rect(rect.x0, rect.y0, rect.x1, rect.y0 + 1)
                page.set_cropbox(new_rect)
                page.set_mediabox(new_rect)
            else:
                page.add_redact_annot(bottom_strip, fill=(1, 1, 1))
                page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)

        # Save to temp file first
        doc.save(tmp_out, deflate=True, garbage=4)
    finally:
        doc.close()

    # Validate the temp output opens
    test = None
    try:
        test = fitz.open(tmp_out)
        _ = len(test)
    finally:
        if test is not None:
            try: test.close()
            except Exception: pass

    # Replace destination atomically with retry (handles brief locks by AV / Explorer Preview)
    last_err = None
    for attempt in range(10):  # ~5s total
        try:
            # os.replace overwrites if dest exists, atomic on same volume
            os.replace(tmp_out, out_path)
            last_err = None
            break
        except PermissionError as e:
            last_err = e
            time.sleep(0.5 + 0.2 * attempt)  # backoff
        except OSError as e:
            # Shouldn't happen since tmp_out and out_path are on same dir/drive now
            last_err = e
            time.sleep(0.5 + 0.2 * attempt)
    if last_err:
        # Clean up temp file if replace ultimately failed
        try: os.remove(tmp_out)
        except Exception: pass
        raise PermissionError(
            f"Could not write output {out_path} (destination in use). "
            f"Close Acrobat/Edge and disable Explorer Preview Pane, then retry. "
            f"Last error: {last_err}"
        )



def main(argv=None):
    argv = argv or sys.argv[1:]

    # Allow simple: script.py input.pdf  -> auto output
    # or: script.py input.pdf output.pdf [flags]
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("input_pdf", nargs="?")
    pre_parser.add_argument("output_pdf", nargs="?")
    known, remaining = pre_parser.parse_known_args(argv)

    parser = argparse.ArgumentParser(description="Remove bottom watermark strip from each PDF page.")
    parser.add_argument("--strip", type=float, default=100, help="Strip height (default: 100)")
    parser.add_argument("--units", choices=["pixels", "points"], default="pixels",
                        help="Units for --strip (default: pixels)")
    parser.add_argument("--dpi", type=float, default=72.0,
                        help="DPI used to convert pixels to points (default: 72)")
    parser.add_argument("--mode", choices=["redact", "crop"], default="redact",
                        help="redact=cover/remove content (keep page size), crop=trim page (default: redact)")
    args = parser.parse_args(remaining)

    if not known.input_pdf:
        print("Usage: python remove_bottom_watermark.py input.pdf [output.pdf] [--strip 100 --units pixels --dpi 72 --mode redact]")
        sys.exit(2)

    in_path = Path(known.input_pdf)
    out_path = Path(known.output_pdf) if known.output_pdf else in_path.with_name(in_path.stem + "_cleaned.pdf")

    process_pdf(in_path, out_path, args.strip, args.units, args.dpi, args.mode)
    print(f"Done: {out_path}")


if __name__ == "__main__":
    main()
