import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import fitz
import requests
from dotenv import load_dotenv
from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

LAYOUT_TABLE_NAME = "digemid_documento_layout_paginas"
LAYOUT_METHOD = "pymupdf_layout"
ALLOWED_STATUSES = [
    "text_extracted",
    "text_extracted_no_products",
    "structured_extracted",
]


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    return create_client(url, key)


def sanitize_file_name(document_key: str | None, file_name: str | None) -> str:
    base_name = file_name or f"{document_key or 'documento'}.pdf"
    safe_name = base_name.replace("/", "-").replace("\\", "-").strip()

    if not safe_name.lower().endswith(".pdf"):
        safe_name = f"{safe_name}.pdf"

    return safe_name


def get_documents_to_process(supabase, limit: int, force: bool) -> list[dict]:
    response = (
        supabase
        .table("digemid_documentos")
        .select("id, document_key, title, file_url, file_name, process_status")
        .eq("source_type", "alerta")
        .eq("has_file", True)
        .not_.is_("file_url", "null")
        .in_("process_status", ALLOWED_STATUSES)
        .order("published_date", desc=True)
        .limit(limit)
        .execute()
    )

    documents = response.data or []
    if force or not documents:
        return documents

    document_ids = [document["id"] for document in documents]
    layout_response = (
        supabase
        .table(LAYOUT_TABLE_NAME)
        .select("document_id")
        .in_("document_id", document_ids)
        .execute()
    )

    existing_ids = {
        row["document_id"]
        for row in (layout_response.data or [])
        if row.get("document_id")
    }

    return [
        document for document in documents
        if document["id"] not in existing_ids
    ]


def download_pdf(file_url: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Descargando PDF: %s", file_url)

    response = requests.get(
        file_url,
        timeout=90,
        headers={
            "User-Agent": "RegAlert-DIGEMID-PDFLayoutExtractor/1.0",
        },
    )
    response.raise_for_status()
    local_path.write_bytes(response.content)

    return local_path


def extract_layout_from_pdf(local_path: Path) -> list[dict]:
    extracted_pages: list[dict] = []

    with fitz.open(local_path) as pdf:
        for page_index, page in enumerate(pdf, start=1):
            text_plain = page.get_text("text") or ""
            text_sorted = page.get_text("text", sort=True) or ""
            blocks_json = page.get_text("dict")
            words_raw = page.get_text("words")
            words_json = [
                {
                    "x0": word[0],
                    "y0": word[1],
                    "x1": word[2],
                    "y1": word[3],
                    "text": word[4],
                    "block_no": word[5],
                    "line_no": word[6],
                    "word_no": word[7],
                }
                for word in words_raw
            ]

            blocks_count = len(blocks_json.get("blocks", []))
            words_count = len(words_json)

            extracted_pages.append({
                "page_number": page_index,
                "page_width": float(page.rect.width),
                "page_height": float(page.rect.height),
                "text_plain": text_plain,
                "text_sorted": text_sorted,
                "blocks_json": blocks_json,
                "words_json": words_json,
                "layout_json": {
                    "blocks_count": blocks_count,
                    "words_count": words_count,
                    "text_plain_length": len(text_plain.strip()),
                    "text_sorted_length": len(text_sorted.strip()),
                },
                "has_layout": True,
                "blocks_count": blocks_count,
                "words_count": words_count,
            })

    return extracted_pages


def get_existing_layout_pages(supabase, document_id: str) -> dict[int, str]:
    response = (
        supabase
        .table(LAYOUT_TABLE_NAME)
        .select("id, page_number")
        .eq("document_id", document_id)
        .execute()
    )

    return {
        int(row["page_number"]): row["id"]
        for row in (response.data or [])
        if row.get("id") and row.get("page_number") is not None
    }


def build_page_payload(document_id: str, page: dict) -> dict:
    return {
        "document_id": document_id,
        "page_number": page["page_number"],
        "page_width": page["page_width"],
        "page_height": page["page_height"],
        "text_plain": page["text_plain"],
        "text_sorted": page["text_sorted"],
        "blocks_json": page["blocks_json"],
        "words_json": page["words_json"],
        "layout_json": page["layout_json"],
        "extraction_method": LAYOUT_METHOD,
        "has_layout": page["has_layout"],
    }


def upsert_layout_pages(supabase, document_id: str, pages: list[dict]) -> None:
    existing_pages = get_existing_layout_pages(supabase, document_id)

    for page in pages:
        payload = build_page_payload(document_id, page)
        existing_page_id = existing_pages.get(page["page_number"])

        if existing_page_id:
            (
                supabase
                .table(LAYOUT_TABLE_NAME)
                .update(payload)
                .eq("id", existing_page_id)
                .execute()
            )
            continue

        (
            supabase
            .table(LAYOUT_TABLE_NAME)
            .insert(payload)
            .execute()
        )


def update_document_after_success(supabase, row: dict, pages: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    total_blocks = sum(page["blocks_count"] for page in pages)
    total_words = sum(page["words_count"] for page in pages)

    (
        supabase
        .table("digemid_documentos")
        .update({
            "process_message": (
                f"Layout visual extraido con {LAYOUT_METHOD}. "
                f"Paginas: {len(pages)}. Bloques: {total_blocks}. Palabras: {total_words}."
            ),
            "updated_at": now,
        })
        .eq("id", row["id"])
        .execute()
    )


def update_document_after_error(supabase, row: dict, error: Exception) -> None:
    now = datetime.now(timezone.utc).isoformat()

    (
        supabase
        .table("digemid_documentos")
        .update({
            "process_message": f"layout_error: {error}",
            "updated_at": now,
        })
        .eq("id", row["id"])
        .execute()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    load_env()

    supabase = get_supabase()
    rows = get_documents_to_process(supabase, args.limit, args.force)

    logger.info("Documentos encontrados para extraer layout: %s", len(rows))

    processed_count = 0
    skipped_count = 0
    error_count = 0
    temp_dir = Path("tmp") / "pdf_layout_extract"

    for row in rows:
        document_id = row["id"]
        document_key = row.get("document_key") or str(document_id)
        file_url = row.get("file_url")
        file_name = sanitize_file_name(document_key, row.get("file_name"))

        try:
            logger.info("Procesando documento: %s", document_key)

            if not file_url:
                logger.info("Saltado %s: no tiene file_url", document_key)
                skipped_count += 1
                continue

            local_path = temp_dir / file_name
            download_pdf(file_url, local_path)
            pages = extract_layout_from_pdf(local_path)

            logger.info("Documento %s | paginas procesadas: %s", document_key, len(pages))
            for page in pages:
                logger.info(
                    "Documento %s | pagina %s | bloques: %s | palabras: %s",
                    document_key,
                    page["page_number"],
                    page["blocks_count"],
                    page["words_count"],
                )

            if args.dry_run:
                logger.info("DRY RUN %s: no se escribira en Supabase", document_key)
                processed_count += 1
                continue

            upsert_layout_pages(supabase, document_id, pages)
            update_document_after_success(supabase, row, pages)

            processed_count += 1
            logger.info("Documento procesado correctamente: %s", document_key)

        except Exception as error:
            error_count += 1
            logger.exception("Error procesando %s: %s", document_key, error)

            if not args.dry_run:
                update_document_after_error(supabase, row, error)

    logger.info(
        "Finalizado. Procesados: %s | Saltados: %s | Errores: %s",
        processed_count,
        skipped_count,
        error_count,
    )


if __name__ == "__main__":
    main()
