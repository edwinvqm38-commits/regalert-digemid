import argparse
import io
import json
import logging
import os
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import fitz
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from supabase import create_client


SCOPES = ["https://www.googleapis.com/auth/drive"]
EXTRACTION_VERSION = "normativa_text_extraction_v1"
REPORTS_DIR = Path("reports")
DRY_RUN_REPORT_PATH = REPORTS_DIR / "normativa_text_extraction_dry_run.md"
RESULT_REPORT_PATH = REPORTS_DIR / "normativa_text_extraction_result.md"
RESULT_JSON_PATH = REPORTS_DIR / "normativa_text_extraction_result.json"
MIN_PDF_BYTES = 10 * 1024
PAGE_TABLE_NAME = "digemid_norma_paginas"
ASSET_TABLE_NAME = "digemid_norma_assets"
NORMA_TABLE_NAME = "digemid_normas"
NORMA_SELECT_FIELDS = (
    "id, document_key, titulo, raw, drive_structure, process_status, updated_at"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def get_drive_service():
    client_path_raw = os.getenv("GOOGLE_OAUTH_CLIENT_JSON_PATH")
    token_path_raw = os.getenv("GOOGLE_OAUTH_TOKEN_PATH")
    if not client_path_raw:
        raise ValueError("Falta GOOGLE_OAUTH_CLIENT_JSON_PATH")
    if not token_path_raw:
        raise ValueError("Falta GOOGLE_OAUTH_TOKEN_PATH")

    client_path = Path(client_path_raw)
    token_path = Path(token_path_raw)
    if not client_path.exists():
        raise FileNotFoundError(f"No existe GOOGLE_OAUTH_CLIENT_JSON_PATH: {client_path}")
    if not token_path.exists():
        raise FileNotFoundError(f"No existe GOOGLE_OAUTH_TOKEN_PATH: {token_path}")

    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
        else:
            raise ValueError("El token OAuth no es valido o requiere reautorizacion")

    return build("drive", "v3", credentials=creds)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--document-key", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if args.dry_run and args.apply:
        raise ValueError("No puedes usar --dry-run y --apply al mismo tiempo")

    if args.apply:
        args.mode = "apply"
    else:
        args.mode = "dry-run"
        args.dry_run = True

    return args


def normalize_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def normalize_page_text(raw_text: str) -> str:
    text = (raw_text or "").replace("\xa0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = "\n".join(line.strip() for line in text.split("\n"))
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def simplify_page_text(raw_text: str) -> str:
    return normalize_text(normalize_page_text(raw_text))


def has_useful_text(text: str) -> bool:
    return bool(normalize_text(text))


def deep_merge_dicts(base: dict, patch: dict) -> dict:
    result = deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def detect_table_like_text(text: str) -> bool:
    if not text:
        return False
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return False

    separated_lines = 0
    numeric_column_pattern = re.compile(r"\b\d[\d.,]*\s{2,}\d[\d.,]*\b")
    for line in lines:
        if "\t" in line or "|" in line or re.search(r"\s{2,}\S+\s{2,}", line):
            separated_lines += 1
            continue
        if numeric_column_pattern.search(line):
            separated_lines += 1

    return separated_lines >= 2


def ensure_reports_dir():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def get_norma_by_document_key(supabase, document_key: str) -> dict:
    response = (
        supabase.table(NORMA_TABLE_NAME)
        .select(NORMA_SELECT_FIELDS)
        .eq("document_key", document_key)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise ValueError(f"No existe digemid_normas.document_key={document_key}")
    return rows[0]


def get_pdf_assets(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table(ASSET_TABLE_NAME)
        .select("id, norma_id, asset_tipo, asset_subtipo, drive_file_id, file_name, mime_type")
        .eq("norma_id", norma_id)
        .eq("asset_tipo", "pdf_original")
        .not_.is_("drive_file_id", "null")
        .order("id", desc=False)
        .execute()
    )
    return response.data or []


def get_existing_pages_by_asset(supabase, norma_id: str, source_asset_id: int) -> dict[int, dict]:
    response = (
        supabase.table(PAGE_TABLE_NAME)
        .select("id, page_number, source_asset_id")
        .eq("norma_id", norma_id)
        .eq("source_asset_id", source_asset_id)
        .execute()
    )
    page_rows = response.data or []
    return {
        int(row["page_number"]): row
        for row in page_rows
        if row.get("id") and row.get("page_number") is not None
    }


def download_drive_file_bytes(service, drive_file_id: str) -> bytes:
    request = service.files().get_media(fileId=drive_file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()


def validate_pdf_bytes(content: bytes):
    if len(content) <= MIN_PDF_BYTES:
        raise ValueError(f"PDF demasiado pequeno: {len(content)} bytes")
    if not content.startswith(b"%PDF"):
        raise ValueError("Magic bytes invalidos: no inicia con %PDF")


def extract_pages_from_pdf_bytes(pdf_bytes: bytes, source_asset_id: int, source_asset_file_name: str, source_asset_subtipo: str | None) -> list[dict]:
    extracted_at = datetime.now(timezone.utc).isoformat()
    pages: list[dict] = []

    with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf:
        for page_number, page in enumerate(pdf, start=1):
            text_raw = normalize_page_text(page.get_text("text") or "")
            text_normalized = simplify_page_text(text_raw)
            page_text_length = len(text_normalized)
            page_word_count = len(text_normalized.split()) if text_normalized else 0

            pages.append(
                {
                    "page_number": page_number,
                    "text_raw": text_raw,
                    "text_normalized": text_normalized,
                    "has_tables": detect_table_like_text(text_raw),
                    "metadata": {
                        "source_asset_id": source_asset_id,
                        "source_asset_file_name": source_asset_file_name,
                        "source_asset_subtipo": source_asset_subtipo,
                        "page_text_length": page_text_length,
                        "page_word_count": page_word_count,
                        "extracted_at": extracted_at,
                        "extraction_version": EXTRACTION_VERSION,
                    },
                }
            )

    return pages


def build_page_payload(norma_id: str, asset: dict, page: dict) -> dict:
    asset_subtipo = normalize_text(asset.get("asset_subtipo")) or None
    file_name = normalize_text(asset.get("file_name")) or f"asset_{asset['id']}.pdf"
    document_part = asset_subtipo or file_name
    return {
        "norma_id": norma_id,
        "source_asset_id": asset["id"],
        "asset_subtipo": asset_subtipo,
        "document_part": document_part,
        "page_number": page["page_number"],
        "text_raw": page["text_raw"],
        "text_normalized": page["text_normalized"],
        "extraction_method": "pymupdf",
        "ocr_used": False,
        "has_tables": page["has_tables"],
        "metadata": page["metadata"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def write_page_record(
    supabase,
    payload: dict,
    existing: dict | None,
    apply_changes: bool,
    force: bool,
    operations: list[dict],
) -> str:
    page_number = payload["page_number"]
    source_asset_id = payload["source_asset_id"]

    if existing and not force:
        operations.append(
            {
                "action": "reuse_page",
                "page_id": existing.get("id"),
                "source_asset_id": source_asset_id,
                "page_number": page_number,
            }
        )
        return "reused"

    if existing and force:
        operations.append(
            {
                "action": "update_page",
                "page_id": existing.get("id"),
                "source_asset_id": source_asset_id,
                "page_number": page_number,
            }
        )
        if not apply_changes:
            return "planned_update"

        supabase.table(PAGE_TABLE_NAME).update(payload).eq("id", existing["id"]).execute()
        return "updated"

    operations.append(
        {
            "action": "insert_page",
            "source_asset_id": source_asset_id,
            "page_number": page_number,
        }
    )
    if not apply_changes:
        return "planned_insert"

    supabase.table(PAGE_TABLE_NAME).insert(payload).execute()
    return "inserted"


def process_asset(
    service,
    supabase,
    norma_id: str,
    asset: dict,
    apply_changes: bool,
    force: bool,
    operations: list[dict],
) -> dict:
    asset_result = {
        "asset_id": asset.get("id"),
        "asset_subtipo": asset.get("asset_subtipo"),
        "file_name": asset.get("file_name"),
        "drive_file_id": asset.get("drive_file_id"),
        "status": "pending",
        "requires_ocr": False,
        "ocr_reason": None,
        "pdf_size_bytes": None,
        "pages_total": 0,
        "pages_with_text": 0,
        "pages_without_text": 0,
        "skipped_empty_pages": 0,
        "pages_inserted": 0,
        "pages_updated": 0,
        "pages_reused": 0,
        "pages_planned_insert": 0,
        "pages_planned_update": 0,
        "total_text_chars": 0,
        "errors": [],
    }

    drive_file_id = normalize_text(asset.get("drive_file_id"))
    if not drive_file_id:
        asset_result["errors"].append("Asset sin drive_file_id")
        asset_result["status"] = "error"
        return asset_result

    pdf_bytes = download_drive_file_bytes(service, drive_file_id)
    validate_pdf_bytes(pdf_bytes)
    asset_result["pdf_size_bytes"] = len(pdf_bytes)

    source_asset_file_name = normalize_text(asset.get("file_name")) or f"asset_{asset['id']}.pdf"
    source_asset_subtipo = normalize_text(asset.get("asset_subtipo")) or None
    pages = extract_pages_from_pdf_bytes(
        pdf_bytes=pdf_bytes,
        source_asset_id=asset["id"],
        source_asset_file_name=source_asset_file_name,
        source_asset_subtipo=source_asset_subtipo,
    )
    asset_result["pages_total"] = len(pages)
    asset_result["pages_with_text"] = sum(
        1 for page in pages if has_useful_text(page["text_normalized"])
    )
    asset_result["pages_without_text"] = (
        asset_result["pages_total"] - asset_result["pages_with_text"]
    )
    asset_result["total_text_chars"] = sum(
        len(page["text_normalized"])
        for page in pages
        if has_useful_text(page["text_normalized"])
    )

    if asset_result["pages_total"] > 0 and asset_result["total_text_chars"] == 0:
        asset_result["requires_ocr"] = True
        asset_result["ocr_reason"] = "PDF sin capa de texto extraible"
        asset_result["status"] = "ocr_required"
        asset_result["skipped_empty_pages"] = asset_result["pages_total"]
        return asset_result

    existing_pages = get_existing_pages_by_asset(
        supabase=supabase,
        norma_id=norma_id,
        source_asset_id=asset["id"],
    )

    for page in pages:
        if not has_useful_text(page["text_normalized"]):
            asset_result["skipped_empty_pages"] += 1
            operations.append(
                {
                    "action": "skip_empty_page",
                    "source_asset_id": asset["id"],
                    "page_number": page["page_number"],
                    "reason": "empty_text",
                }
            )
            continue

        page_number = page["page_number"]
        payload = build_page_payload(norma_id=norma_id, asset=asset, page=page)
        existing = existing_pages.get(page_number)
        try:
            action = write_page_record(
                supabase=supabase,
                payload=payload,
                existing=existing,
                apply_changes=apply_changes,
                force=force,
                operations=operations,
            )
            if action == "inserted":
                asset_result["pages_inserted"] += 1
            elif action == "updated":
                asset_result["pages_updated"] += 1
            elif action == "reused":
                asset_result["pages_reused"] += 1
            elif action == "planned_insert":
                asset_result["pages_planned_insert"] += 1
            elif action == "planned_update":
                asset_result["pages_planned_update"] += 1
        except Exception as exc:
            asset_result["errors"].append(
                f"pagina {page_number}: {exc}"
            )

    if asset_result["errors"]:
        asset_result["status"] = "error"
    elif asset_result["pages_with_text"] > 0 and asset_result["pages_without_text"] > 0:
        asset_result["requires_ocr"] = True
        asset_result["ocr_reason"] = "PDF con paginas mixtas: algunas sin texto extraible"
        asset_result["status"] = "partial_text"
    else:
        asset_result["status"] = "text_extracted"

    return asset_result


def summarize_results(asset_results: list[dict]) -> dict:
    summary = {
        "pdf_assets_total": len(asset_results),
        "pdf_assets_processed": 0,
        "total_pages_extracted": 0,
        "total_text_chars": 0,
        "pages_with_text": 0,
        "pages_without_text": 0,
        "skipped_empty_pages": 0,
        "pages_inserted": 0,
        "pages_updated": 0,
        "pages_reused": 0,
        "pages_planned_insert": 0,
        "pages_planned_update": 0,
        "requires_ocr": False,
        "errors_count": 0,
    }
    for item in asset_results:
        has_errors = bool(item.get("errors"))
        if not has_errors and item.get("status") != "ocr_required":
            summary["pdf_assets_processed"] += 1
        summary["total_pages_extracted"] += item.get("pages_total", 0)
        summary["total_text_chars"] += item.get("total_text_chars", 0)
        summary["pages_with_text"] += item.get("pages_with_text", 0)
        summary["pages_without_text"] += item.get("pages_without_text", 0)
        summary["skipped_empty_pages"] += item.get("skipped_empty_pages", 0)
        summary["pages_inserted"] += item.get("pages_inserted", 0)
        summary["pages_updated"] += item.get("pages_updated", 0)
        summary["pages_reused"] += item.get("pages_reused", 0)
        summary["pages_planned_insert"] += item.get("pages_planned_insert", 0)
        summary["pages_planned_update"] += item.get("pages_planned_update", 0)
        summary["requires_ocr"] = summary["requires_ocr"] or bool(item.get("requires_ocr"))
        summary["errors_count"] += len(item.get("errors", []))
    return summary


def build_norma_update_payload(norma_row: dict, summary: dict, text_extracted_at: str, status: str) -> dict:
    existing_raw = norma_row.get("raw") if isinstance(norma_row.get("raw"), dict) else {}
    raw_patch = {
        "text_extraction_version": EXTRACTION_VERSION,
        "text_extracted_at": text_extracted_at,
        "pdf_assets_processed": summary["pdf_assets_processed"],
        "total_pages_extracted": summary["total_pages_extracted"],
        "total_text_chars": summary["total_text_chars"],
        "pages_with_text": summary["pages_with_text"],
        "pages_without_text": summary["pages_without_text"],
        "ocr_required": summary["requires_ocr"],
        "errors_count": summary["errors_count"],
    }
    merged_raw = deep_merge_dicts(existing_raw, raw_patch)
    return {
        "raw": merged_raw,
        "ocr_required": summary["requires_ocr"],
        "process_status": status,
        "updated_at": text_extracted_at,
    }


def write_report_files(mode: str, report_payload: dict):
    ensure_reports_dir()
    RESULT_JSON_PATH.write_text(
        json.dumps(report_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary = report_payload.get("summary", {})
    lines = [
        f"# DIGEMID Normativa Text Extraction - {mode}",
        "",
        f"- Mode: `{mode}`",
        f"- Document key: `{report_payload.get('document_key')}`",
        f"- Force: `{report_payload.get('force')}`",
        f"- PDF assets total: **{summary.get('pdf_assets_total', 0)}**",
        f"- PDF assets processed ok: **{summary.get('pdf_assets_processed', 0)}**",
        f"- Total pages extracted: **{summary.get('total_pages_extracted', 0)}**",
        f"- Total text chars: **{summary.get('total_text_chars', 0)}**",
        f"- Pages with text / without text: **{summary.get('pages_with_text', 0)} / {summary.get('pages_without_text', 0)}**",
        f"- Skipped empty pages: **{summary.get('skipped_empty_pages', 0)}**",
        f"- Requires OCR: `{summary.get('requires_ocr', False)}`",
        f"- Planned process_status: `{report_payload.get('planned_process_status')}`",
        f"- Pages inserted/updated/reused: **{summary.get('pages_inserted', 0)} / {summary.get('pages_updated', 0)} / {summary.get('pages_reused', 0)}**",
        f"- Planned inserts/updates: **{summary.get('pages_planned_insert', 0)} / {summary.get('pages_planned_update', 0)}**",
        f"- Errors: **{summary.get('errors_count', 0)}**",
        "",
        "## PDFs",
        "",
    ]

    for item in report_payload.get("assets", []):
        lines.append(
            f"### asset_id={item.get('asset_id')} | subtipo={item.get('asset_subtipo') or ''}"
        )
        lines.append(f"- file_name: `{item.get('file_name') or ''}`")
        lines.append(f"- drive_file_id: `{item.get('drive_file_id') or ''}`")
        lines.append(f"- status: `{item.get('status')}`")
        lines.append(f"- requires_ocr: `{item.get('requires_ocr')}`")
        lines.append(f"- ocr_reason: `{item.get('ocr_reason') or ''}`")
        lines.append(f"- pdf_size_bytes: `{item.get('pdf_size_bytes')}`")
        lines.append(f"- pages_total: `{item.get('pages_total')}`")
        lines.append(
            f"- pages_with_text / pages_without_text: `{item.get('pages_with_text')}` / `{item.get('pages_without_text')}`"
        )
        lines.append(f"- skipped_empty_pages: `{item.get('skipped_empty_pages')}`")
        lines.append(
            f"- pages inserted/updated/reused: `{item.get('pages_inserted')}` / `{item.get('pages_updated')}` / `{item.get('pages_reused')}`"
        )
        lines.append(
            f"- pages planned insert/update: `{item.get('pages_planned_insert')}` / `{item.get('pages_planned_update')}`"
        )
        lines.append(f"- total_text_chars: `{item.get('total_text_chars')}`")
        if item.get("errors"):
            lines.append("- errors:")
            for err in item["errors"]:
                lines.append(f"  - {err}")
        lines.append("")

    target_path = DRY_RUN_REPORT_PATH if mode == "dry-run" else RESULT_REPORT_PATH
    target_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    other_path = RESULT_REPORT_PATH if mode == "dry-run" else DRY_RUN_REPORT_PATH
    if not other_path.exists():
        other_path.write_text(
            f"# DIGEMID Normativa Text Extraction - {mode}\n\nEste reporte no fue generado en esta ejecucion.\n",
            encoding="utf-8",
        )


def process_norma(service, supabase, norma_row: dict, apply_changes: bool, force: bool) -> dict:
    operations: list[dict] = []
    assets = get_pdf_assets(supabase, norma_row["id"])
    if not assets:
        raise ValueError(
            f"No hay assets pdf_original con drive_file_id para norma {norma_row.get('document_key')}"
        )

    asset_results: list[dict] = []
    for asset in assets:
        try:
            logger.info(
                "Procesando asset PDF %s (%s)",
                asset.get("id"),
                asset.get("asset_subtipo") or asset.get("file_name"),
            )
            result = process_asset(
                service=service,
                supabase=supabase,
                norma_id=norma_row["id"],
                asset=asset,
                apply_changes=apply_changes,
                force=force,
                operations=operations,
            )
        except Exception as exc:
            result = {
                "asset_id": asset.get("id"),
                "asset_subtipo": asset.get("asset_subtipo"),
                "file_name": asset.get("file_name"),
                "drive_file_id": asset.get("drive_file_id"),
                "pdf_size_bytes": None,
                "pages_total": 0,
                "pages_inserted": 0,
                "pages_updated": 0,
                "pages_reused": 0,
                "pages_planned_insert": 0,
                "pages_planned_update": 0,
                "total_text_chars": 0,
                "errors": [str(exc)],
            }
        asset_results.append(result)

    summary = summarize_results(asset_results)
    text_extracted_at = datetime.now(timezone.utc).isoformat()
    assets_with_text = sum(1 for item in asset_results if item.get("pages_with_text", 0) > 0)
    assets_without_text = sum(
        1
        for item in asset_results
        if item.get("pages_total", 0) > 0 and item.get("total_text_chars", 0) == 0
    )
    all_assets_ok = summary["errors_count"] == 0 and assets_with_text == summary["pdf_assets_total"]

    if summary["total_text_chars"] == 0 and summary["total_pages_extracted"] > 0:
        process_status = "ocr_required"
    elif assets_with_text > 0 and (assets_without_text > 0 or summary["errors_count"] > 0):
        process_status = "text_extraction_partial"
    elif all_assets_ok:
        process_status = "text_extracted"
    else:
        process_status = "text_extraction_partial"

    operations.append(
        {
            "action": "update_norma_status",
            "norma_id": norma_row["id"],
            "document_key": norma_row.get("document_key"),
            "process_status": process_status,
        }
    )
    if apply_changes:
        payload = build_norma_update_payload(
            norma_row=norma_row,
            summary=summary,
            text_extracted_at=text_extracted_at,
            status=process_status,
        )
        supabase.table(NORMA_TABLE_NAME).update(payload).eq("id", norma_row["id"]).execute()

    return {
        "norma_id": norma_row["id"],
        "document_key": norma_row.get("document_key"),
        "title": norma_row.get("titulo"),
        "force": force,
        "mode": "apply" if apply_changes else "dry-run",
        "planned_process_status": process_status,
        "summary": summary,
        "assets": asset_results,
        "operations": operations,
    }


def main():
    args = parse_args()
    load_env()

    supabase = get_supabase()
    drive_service = get_drive_service()
    norma_row = get_norma_by_document_key(supabase, args.document_key)
    result = process_norma(
        service=drive_service,
        supabase=supabase,
        norma_row=norma_row,
        apply_changes=args.mode == "apply",
        force=args.force,
    )

    report_payload = {
        "mode": args.mode,
        "document_key": args.document_key,
        "force": args.force,
        "extraction_version": EXTRACTION_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        **result,
    }
    write_report_files(args.mode, report_payload)
    logger.info("Reportes generados en %s", REPORTS_DIR)


if __name__ == "__main__":
    main()
