import argparse
import logging
import os
import re
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


NORMA_TABLE_NAME = "digemid_normas"
ASSET_TABLE_NAME = "digemid_norma_assets"
PAGE_TABLE_NAME = "digemid_norma_paginas"
DEFAULT_OUTPUT_DIR = "exports"

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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--document-key", required=True)
    parser.add_argument("--format", required=True, choices=["md", "txt"])
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--source", required=True, choices=["raw", "normalized"])
    return parser.parse_args()


def normalize_text(value):
    if value is None:
        return ""
    return str(value).replace("\r\n", "\n").replace("\r", "\n")


def sanitize_file_component(value: str) -> str:
    cleaned = re.sub(r"[<>:\"/\\|?*]+", "-", value).strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned or "norma"


def get_norma_by_document_key(supabase, document_key: str) -> dict:
    response = (
        supabase.table(NORMA_TABLE_NAME)
        .select("id, document_key, titulo, process_status, ocr_required")
        .eq("document_key", document_key)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise ValueError(f"No existe digemid_normas.document_key={document_key}")
    return rows[0]


def get_assets_for_norma(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table(ASSET_TABLE_NAME)
        .select("id, asset_subtipo, file_name")
        .eq("norma_id", norma_id)
        .order("id", desc=False)
        .execute()
    )
    return response.data or []


def get_pages_for_norma(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table(PAGE_TABLE_NAME)
        .select(
            "id, source_asset_id, page_number, text_raw, text_normalized, "
            "ocr_used, extraction_method"
        )
        .eq("norma_id", norma_id)
        .order("source_asset_id", desc=False)
        .order("page_number", desc=False)
        .execute()
    )
    return response.data or []


def build_asset_index(assets: list[dict]) -> OrderedDict[int | None, dict]:
    ordered = OrderedDict()
    for asset in assets:
        asset_id = asset.get("id")
        ordered[asset_id] = {
            "asset_id": asset_id,
            "asset_subtipo": asset.get("asset_subtipo"),
            "file_name": asset.get("file_name"),
            "pages": [],
        }
    return ordered


def group_pages_by_asset(assets: list[dict], pages: list[dict], source_field: str) -> OrderedDict:
    grouped = build_asset_index(assets)

    for page in pages:
        asset_id = page.get("source_asset_id")
        bucket = grouped.get(asset_id)
        if bucket is None:
            bucket = {
                "asset_id": asset_id,
                "asset_subtipo": None,
                "file_name": None,
                "pages": [],
            }
            grouped[asset_id] = bucket

        text_value = normalize_text(page.get(source_field))
        text_for_export = text_value.strip()
        bucket["pages"].append(
            {
                "page_number": page.get("page_number"),
                "ocr_used": bool(page.get("ocr_used")),
                "extraction_method": page.get("extraction_method") or "",
                "text": text_for_export,
                "text_chars": len(text_for_export),
            }
        )

    return grouped


def compute_summary(grouped_assets: OrderedDict) -> dict:
    assets_with_pages = 0
    pages_exported = 0
    ocr_pages = 0
    total_chars = 0

    for asset in grouped_assets.values():
        if asset["pages"]:
            assets_with_pages += 1
        for page in asset["pages"]:
            pages_exported += 1
            if page["ocr_used"]:
                ocr_pages += 1
            total_chars += page["text_chars"]

    return {
        "assets_count": assets_with_pages,
        "pages_exported": pages_exported,
        "ocr_pages": ocr_pages,
        "total_chars": total_chars,
    }


def format_page_text(text: str) -> str:
    return text if text else "[SIN TEXTO REGISTRADO]"


def render_markdown(norma: dict, grouped_assets: OrderedDict, summary: dict, exported_at: str, source: str) -> str:
    lines = [
        "# Norma",
        "",
        f"- Título: {norma.get('titulo') or ''}",
        f"- document_key: `{norma.get('document_key')}`",
        f"- process_status: `{norma.get('process_status')}`",
        f"- ocr_required: `{norma.get('ocr_required')}`",
        f"- fecha_exportación: `{exported_at}`",
        f"- source: `{source}`",
        "",
        "## Resumen General",
        "",
        f"- cantidad de assets: **{summary['assets_count']}**",
        f"- cantidad de páginas exportadas: **{summary['pages_exported']}**",
        f"- cantidad de páginas OCR: **{summary['ocr_pages']}**",
        f"- caracteres totales: **{summary['total_chars']}**",
        "",
    ]

    for asset in grouped_assets.values():
        if not asset["pages"]:
            continue
        lines.extend(
            [
                "## PDF / Asset",
                "",
                f"- asset_id: `{asset['asset_id']}`",
                f"- asset_subtipo: `{asset.get('asset_subtipo') or ''}`",
                f"- file_name: `{asset.get('file_name') or ''}`",
                "",
            ]
        )
        for page in asset["pages"]:
            lines.extend(
                [
                    f"### Página {page['page_number']}",
                    "",
                    f"- page_number: `{page['page_number']}`",
                    f"- ocr_used: `{page['ocr_used']}`",
                    f"- extraction_method: `{page['extraction_method']}`",
                    f"- cantidad de caracteres: `{page['text_chars']}`",
                    "",
                    format_page_text(page["text"]),
                    "",
                    "---",
                    "",
                ]
            )

    return "\n".join(lines).strip() + "\n"


def render_txt(norma: dict, grouped_assets: OrderedDict, summary: dict, exported_at: str, source: str) -> str:
    separator = "=" * 80
    lines = [
        separator,
        "NORMA",
        separator,
        f"Titulo: {norma.get('titulo') or ''}",
        f"document_key: {norma.get('document_key')}",
        f"process_status: {norma.get('process_status')}",
        f"ocr_required: {norma.get('ocr_required')}",
        f"fecha_exportacion: {exported_at}",
        f"source: {source}",
        "",
        "RESUMEN GENERAL",
        separator,
        f"cantidad de assets: {summary['assets_count']}",
        f"cantidad de paginas exportadas: {summary['pages_exported']}",
        f"cantidad de paginas OCR: {summary['ocr_pages']}",
        f"caracteres totales: {summary['total_chars']}",
        "",
    ]

    for asset in grouped_assets.values():
        if not asset["pages"]:
            continue
        lines.extend(
            [
                separator,
                "PDF / ASSET",
                separator,
                f"asset_id: {asset['asset_id']}",
                f"asset_subtipo: {asset.get('asset_subtipo') or ''}",
                f"file_name: {asset.get('file_name') or ''}",
                "",
            ]
        )
        for page in asset["pages"]:
            lines.extend(
                [
                    separator,
                    f"PAGINA {page['page_number']}",
                    separator,
                    f"page_number: {page['page_number']}",
                    f"ocr_used: {page['ocr_used']}",
                    f"extraction_method: {page['extraction_method']}",
                    f"cantidad de caracteres: {page['text_chars']}",
                    "",
                    format_page_text(page["text"]),
                    "",
                ]
            )

    return "\n".join(lines).strip() + "\n"


def build_output_path(output_dir: str, document_key: str, source: str, extension: str) -> Path:
    dir_path = Path(output_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_name = f"{sanitize_file_component(document_key)}__{source}.{extension}"
    return dir_path / file_name


def export_norma_text(supabase, document_key: str, output_dir: str, fmt: str, source: str) -> dict:
    norma = get_norma_by_document_key(supabase, document_key)
    assets = get_assets_for_norma(supabase, norma["id"])
    pages = get_pages_for_norma(supabase, norma["id"])

    if not pages:
        message = (
            f"No hay páginas registradas en {PAGE_TABLE_NAME} para la norma "
            f"{document_key}."
        )
        logger.warning(message)
        return {
            "ok": False,
            "message": message,
            "document_key": document_key,
            "output_path": None,
            "summary": {
                "assets_count": 0,
                "pages_exported": 0,
                "ocr_pages": 0,
                "total_chars": 0,
            },
        }

    source_field = "text_raw" if source == "raw" else "text_normalized"
    grouped_assets = group_pages_by_asset(assets, pages, source_field)
    summary = compute_summary(grouped_assets)
    exported_at = datetime.now(timezone.utc).isoformat()

    if fmt == "md":
        content = render_markdown(norma, grouped_assets, summary, exported_at, source)
    else:
        content = render_txt(norma, grouped_assets, summary, exported_at, source)

    output_path = build_output_path(output_dir, document_key, source, fmt)
    output_path.write_text(content, encoding="utf-8")

    return {
        "ok": True,
        "message": "Exportación completada.",
        "document_key": document_key,
        "output_path": output_path,
        "summary": summary,
    }


def main():
    args = parse_args()
    load_env()
    supabase = get_supabase()
    result = export_norma_text(
        supabase=supabase,
        document_key=args.document_key,
        output_dir=args.output_dir,
        fmt=args.format,
        source=args.source,
    )

    if not result["ok"]:
        logger.warning(result["message"])
        return

    logger.info(
        "Archivo generado: %s | páginas: %s | assets: %s | páginas OCR: %s",
        result["output_path"],
        result["summary"]["pages_exported"],
        result["summary"]["assets_count"],
        result["summary"]["ocr_pages"],
    )


if __name__ == "__main__":
    main()
