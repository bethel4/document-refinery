from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import pdfplumber


def _clean_cell(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def _normalize_headers(raw_headers: List[Any]) -> List[str]:
    headers: List[str] = []
    seen: Dict[str, int] = {}
    for index, header in enumerate(raw_headers):
        cleaned = _clean_cell(header) or f"column_{index + 1}"
        count = seen.get(cleaned, 0)
        seen[cleaned] = count + 1
        headers.append(cleaned if count == 0 else f"{cleaned}_{count + 1}")
    return headers


def _row_to_dict(headers: List[str], row: List[Any]) -> Dict[str, Any]:
    values = [_clean_cell(value) for value in row]
    if len(values) < len(headers):
        values.extend([""] * (len(headers) - len(values)))
    if len(values) > len(headers):
        values = values[: len(headers)]
    return {header: value for header, value in zip(headers, values)}


def extract_tables_with_pdfplumber(pdf_path: Path) -> List[Dict[str, Any]]:
    extracted_tables: List[Dict[str, Any]] = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []
            for table_index, table in enumerate(tables, start=1):
                if not table or len(table) < 2:
                    continue

                headers = _normalize_headers(table[0])
                rows = []
                for row in table[1:]:
                    if not row:
                        continue
                    row_dict = _row_to_dict(headers, row)
                    if any(value for value in row_dict.values()):
                        rows.append(row_dict)

                if rows:
                    extracted_tables.append(
                        {
                            "page": page_number,
                            "table_index": table_index,
                            "rows": rows,
                        }
                    )

    return extracted_tables


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract PDF tables and save them as JSON rows keyed by table headers."
    )
    parser.add_argument("pdf_path", help="Path to the input PDF file.")
    parser.add_argument(
        "-o",
        "--output",
        help="Path to the output JSON file. Defaults to <pdf_stem>_tables.json in the same directory.",
    )
    parser.add_argument(
        "--flatten",
        action="store_true",
        help="Write a single flat JSON array of rows instead of page/table-grouped output.",
    )
    args = parser.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        raise SystemExit(f"PDF not found: {pdf_path}")

    output_path = Path(args.output) if args.output else pdf_path.with_name(f"{pdf_path.stem}_tables.json")
    tables = extract_tables_with_pdfplumber(pdf_path)

    payload: Any
    if args.flatten:
        payload = [row for table in tables for row in table["rows"]]
    else:
        payload = tables

    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {len(tables)} table(s) to {output_path}")


if __name__ == "__main__":
    main()
