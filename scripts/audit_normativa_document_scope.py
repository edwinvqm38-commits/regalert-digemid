"""CLI local y read-only para la auditoría F04C.1 de pertenencia documental."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from agents.document_scope_audit import (  # noqa: E402
    audit_document_scope,
    render_scope_summary,
    report_to_json,
    scope_page_from_pdf,
    sha256_file,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audita pertenencia documental sin escribir en Supabase ni alterar el PDF."
    )
    parser.add_argument("--pdf", required=True, type=Path)
    parser.add_argument("--document-key", required=True)
    parser.add_argument("--source-asset-id", type=int)
    parser.add_argument(
        "--pages", help="Páginas PDF 1-based, ej. 1,12-13; por defecto todas"
    )
    parser.add_argument(
        "--printed-page-offset",
        type=int,
        default=0,
        help="Suma aplicada a la página física para informar numeración impresa.",
    )
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--summary-output", type=Path)
    return parser.parse_args()


def parse_pages(value: str | None, total: int) -> list[int]:
    if not value:
        return list(range(1, total + 1))
    selected: set[int] = set()
    for part in value.split(","):
        bounds = part.strip().split("-", 1)
        start = int(bounds[0])
        end = int(bounds[1]) if len(bounds) == 2 else start
        if start < 1 or end < start or end > total:
            raise ValueError(f"Rango de páginas inválido: {part}")
        selected.update(range(start, end + 1))
    return sorted(selected)


def main() -> int:
    args = parse_args()
    if not args.pdf.is_file():
        raise FileNotFoundError(args.pdf)

    import fitz

    with fitz.open(args.pdf) as document:
        selected = parse_pages(args.pages, document.page_count)
    pages = [
        scope_page_from_pdf(
            args.pdf,
            page_number - 1,
            printed_page=str(page_number + args.printed_page_offset)
            if args.printed_page_offset
            else None,
        )
        for page_number in selected
    ]
    report = audit_document_scope(
        document_key=args.document_key,
        pdf_name=args.pdf.name,
        pdf_sha256=sha256_file(args.pdf),
        source_asset_id=args.source_asset_id,
        pages=pages,
        evidence_source="local_pdf_layout",
    )
    json_text = report_to_json(report)
    summary = render_scope_summary(report)
    print(summary)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json_text + "\n", encoding="utf-8")
    if args.summary_output:
        args.summary_output.parent.mkdir(parents=True, exist_ok=True)
        args.summary_output.write_text(summary + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
