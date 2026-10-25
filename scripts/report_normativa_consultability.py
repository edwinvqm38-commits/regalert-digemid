"""Reporte read-only de consultabilidad del corpus normativo MVP."""

import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


DEFAULT_DOCUMENT_KEYS = (
    "LEY-29459",
    "DS-014-2011-SA",
    "DS-016-2011-SA",
    "RM-554-2022-MINSA",
    "RM-810-2024-MINSA",
    "DS-008-2025-SA",
    "LEY-32033",
)


def has_text(page: dict) -> bool:
    if page.get("extraction_method") in {"pending_ocr", "verified_blank"}:
        return False
    value = page.get("text_normalized") or page.get("text_raw") or ""
    return bool(str(value).strip())


def page_metadata(page: dict) -> dict:
    return page.get("metadata") if isinstance(page.get("metadata"), dict) else {}


def expected_physical_pages(asset: dict, pages: list[dict]) -> int | None:
    asset_metadata = asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {}
    asset_value = asset_metadata.get("pdf_physical_page_count")
    if isinstance(asset_value, int) and asset_value > 0:
        return asset_value
    values = set()
    for page in pages:
        value = page_metadata(page).get("pdf_physical_page_count")
        if isinstance(value, int) and value > 0:
            values.add(value)
    return next(iter(values)) if len(values) == 1 else None


def asset_source_sha256(asset: dict) -> str:
    metadata = asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {}
    return str(metadata.get("sha256") or "").strip().lower()


def asset_needs_source_validation(asset: dict) -> bool:
    metadata = asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {}
    status = str(metadata.get("source_validation_status") or "").strip().lower()
    return (
        status in {"quarantine", "needs_source_validation"}
        or metadata.get("quarantine") is True
        or metadata.get("needs_source_validation") is True
    )


def classify_asset(asset: dict, pages: list[dict]) -> dict:
    asset_id = asset.get("id")
    expected = expected_physical_pages(asset, pages)
    source_sha256 = asset_source_sha256(asset)
    numbers = [
        int(page["page_number"])
        for page in pages
        if page.get("page_number") is not None
    ]
    duplicate_pages = sorted({number for number in numbers if numbers.count(number) > 1})
    persisted = set(numbers)
    expected_set = set(range(1, expected + 1)) if expected else set()
    missing = sorted(expected_set - persisted)
    unexpected = sorted(persisted - expected_set) if expected else sorted(persisted)
    source_mismatch = []
    textual_pages = []
    verified_blank_pages = []
    pending_pages = []

    for page in pages:
        number = int(page["page_number"])
        metadata = page_metadata(page)
        page_sha = str(metadata.get("source_pdf_sha256") or "").strip().lower()
        source_matches = bool(source_sha256) and page_sha == source_sha256
        physical_number_valid = expected is not None and 1 <= number <= expected
        if not source_matches:
            source_mismatch.append(number)
        if has_text(page) and source_matches and physical_number_valid:
            textual_pages.append(number)
        elif (
            metadata.get("verified_blank") is True
            and page.get("source_asset_id") == asset_id
            and source_matches
            and physical_number_valid
        ):
            verified_blank_pages.append(number)
        else:
            pending_pages.append(number)

    resolved = set(textual_pages) | set(verified_blank_pages)
    quarantine = asset_needs_source_validation(asset)
    complete = bool(
        expected
        and source_sha256
        and not quarantine
        and not missing
        and not unexpected
        and not duplicate_pages
        and not source_mismatch
        and resolved == expected_set
    )
    metadata = asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {}
    return {
        "asset_id": asset_id,
        "file_name": asset.get("file_name"),
        "asset_subtipo": asset.get("asset_subtipo"),
        "required": metadata.get("required_for_consultability") is not False,
        "source_sha256": source_sha256 or None,
        "expected_physical_pages": expected,
        "persisted_pages": len(pages),
        "pages_with_text": len(textual_pages),
        "verified_blank_pages": sorted(set(verified_blank_pages)),
        "pending_pages": sorted(set(pending_pages)),
        "missing_pages": missing,
        "unexpected_pages": unexpected,
        "duplicate_pages": duplicate_pages,
        "source_mismatch_pages": sorted(set(source_mismatch)),
        "needs_source_validation": quarantine,
        "complete": complete,
    }


def classify_norma(norma: dict | None, assets: list[dict], pages: list[dict]) -> dict:
    if not norma:
        return {
            "document_key": None,
            "process_status": None,
            "registered": False,
            "pdf_available": False,
            "pages_registered": False,
            "text_available": False,
            "fidelity_pending": True,
            "needs_source_validation": False,
            "consultable": False,
            "page_count": 0,
            "pages_with_text": 0,
            "pages_ocr": 0,
            "pages_pending": 0,
            "verified_blank_pages": 0,
            "missing_pages": [],
            "missing_pages_by_asset": {},
            "orphan_pages": 0,
            "expected_physical_pages": None,
            "assets": [],
        }

    pdf_assets = [asset for asset in assets if asset.get("asset_tipo") == "pdf_original"]
    asset_ids = {asset.get("id") for asset in pdf_assets}
    asset_results = [
        classify_asset(
            asset,
            [page for page in pages if page.get("source_asset_id") == asset.get("id")],
        )
        for asset in pdf_assets
    ]
    required_assets = [item for item in asset_results if item["required"]]
    orphan_pages = [
        page for page in pages
        if page.get("source_asset_id") not in asset_ids
    ]
    with_text = sum(item["pages_with_text"] for item in required_assets)
    verified_blank_count = sum(
        len(item["verified_blank_pages"]) for item in required_assets
    )
    pending = (
        sum(len(item["pending_pages"]) for item in required_assets)
        + len(orphan_pages)
    )
    ocr_pages = sum(
        1
        for page in pages
        if page.get("ocr_used") is True and page.get("source_asset_id") in asset_ids
    )
    pdf_available = bool(
        pdf_assets
        or norma.get("drive_file_id")
        or norma.get("file_storage_path")
        or norma.get("pdf_url")
    )
    norma_needs_source_validation = str(
        norma.get("process_status") or ""
    ).strip().lower() in {"quarantine", "needs_source_validation"}
    missing_by_asset = {
        str(item["asset_id"]): item["missing_pages"]
        for item in required_assets
        if item["missing_pages"]
    }
    complete = bool(
        required_assets
        and all(item["complete"] for item in required_assets)
        and not orphan_pages
        and with_text > 0
        and not norma_needs_source_validation
    )
    fidelity_pending = (
        norma_needs_source_validation
        or pending > 0
        or bool(missing_by_asset)
        or any(page.get("revisado_manual") is not True for page in pages)
    )
    expected_values = [item["expected_physical_pages"] for item in required_assets]
    expected_total = (
        sum(expected_values)
        if expected_values and all(value is not None for value in expected_values)
        else None
    )
    single_missing = required_assets[0]["missing_pages"] if len(required_assets) == 1 else []

    return {
        "document_key": norma.get("document_key"),
        "process_status": norma.get("process_status"),
        "registered": True,
        "pdf_available": pdf_available,
        "pages_registered": bool(pages),
        "text_available": with_text > 0,
        "fidelity_pending": fidelity_pending,
        "needs_source_validation": norma_needs_source_validation
        or any(item["needs_source_validation"] for item in required_assets),
        "consultable": pdf_available and complete,
        "page_count": len(pages),
        "pages_with_text": with_text,
        "pages_ocr": ocr_pages,
        "pages_pending": pending,
        "verified_blank_pages": verified_blank_count,
        "missing_pages": single_missing,
        "missing_pages_by_asset": missing_by_asset,
        "orphan_pages": len(orphan_pages),
        "expected_physical_pages": expected_total,
        "assets": asset_results,
    }


def fetch_report(supabase, document_keys: list[str]) -> list[dict]:
    normas = (
        supabase.table("digemid_normas")
        .select(
            "id, document_key, process_status, drive_file_id, "
            "file_storage_path, pdf_url"
        )
        .in_("document_key", document_keys)
        .execute()
        .data
        or []
    )
    by_key = {row["document_key"]: row for row in normas}
    ids = [row["id"] for row in normas]
    assets = []
    pages = []
    if ids:
        assets = (
            supabase.table("digemid_norma_assets")
            .select(
                "id, norma_id, asset_tipo, asset_subtipo, drive_file_id, "
                "file_name, metadata"
            )
            .in_("norma_id", ids)
            .execute()
            .data
            or []
        )
        pages = (
            supabase.table("digemid_norma_paginas")
            .select(
                "norma_id, source_asset_id, page_number, text_raw, text_normalized, "
                "ocr_used, revisado_manual, extraction_method, metadata"
            )
            .in_("norma_id", ids)
            .execute()
            .data
            or []
        )

    rows = []
    for key in document_keys:
        norma = by_key.get(key)
        if not norma:
            row = classify_norma(None, [], [])
            row["document_key"] = key
        else:
            row = classify_norma(
                norma,
                [item for item in assets if item.get("norma_id") == norma["id"]],
                [item for item in pages if item.get("norma_id") == norma["id"]],
            )
        rows.append(row)
    return rows


def render_markdown(rows: list[dict]) -> str:
    lines = [
        "# Consultabilidad normativa MVP",
        "",
        "| document_key | process_status | PDF | páginas | con texto | blancas verificadas | OCR | pendientes | consultable |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['document_key']} | {row.get('process_status') or '-'} | "
            f"{'sí' if row['pdf_available'] else 'no'} | {row['page_count']} | "
            f"{row['pages_with_text']} | {row.get('verified_blank_pages', 0)} | "
            f"{row['pages_ocr']} | {row['pages_pending']} | "
            f"{'sí' if row['consultable'] else 'no'} |"
        )
    return "\n".join(lines) + "\n"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--document-keys",
        default=",".join(DEFAULT_DOCUMENT_KEYS),
        help="document_key separados por coma",
    )
    parser.add_argument("--output-prefix", default="reports/normativa_consultability")
    parser.add_argument("--dry-run", action="store_true", help="Compatibilidad: el reporte siempre es read-only")
    return parser.parse_args()


def main():
    args = parse_args()
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    document_keys = [item.strip() for item in args.document_keys.split(",") if item.strip()]
    rows = fetch_report(create_client(url, key), document_keys)
    prefix = Path(args.output_prefix)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    prefix.with_suffix(".json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    prefix.with_suffix(".md").write_text(render_markdown(rows), encoding="utf-8")
    print(render_markdown(rows), end="")


if __name__ == "__main__":
    main()
