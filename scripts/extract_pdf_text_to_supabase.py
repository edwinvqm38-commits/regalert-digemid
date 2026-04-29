import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import fitz
import requests
from dotenv import load_dotenv
from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

PAGE_TABLE_NAME = "digemid_documento_paginas"
TEXT_METHOD = "pymupdf"


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    return create_client(url, key)


def get_documents_without_pages(supabase, limit: int) -> list[dict]:
    response = (
        supabase
        .table("digemid_documentos")
        .select("id, document_key, title, file_url, file_name, process_status")
        .eq("source_type", "alerta")
        .eq("has_file", True)
        .not_.is_("file_url", "null")
        .order("published_date", desc=True)
        .limit(limit)
        .execute()
    )

    documents = response.data or []

    if not documents:
        return []

    document_ids = [document["id"] for document in documents]
    pages_response = (
        supabase
        .table(PAGE_TABLE_NAME)
        .select("document_id")
        .in_("document_id", document_ids)
        .execute()
    )

    existing_ids = {
        row["document_id"]
        for row in (pages_response.data or [])
        if row.get("document_id")
    }

    return [
        document for document in documents
        if document["id"] not in existing_ids
    ]


def sanitize_file_name(document_key: str | None, file_name: str | None) -> str:
    base_name = file_name or f"{document_key or 'documento'}.pdf"
    safe_name = base_name.replace("/", "-").replace("\\", "-").strip()

    if not safe_name.lower().endswith(".pdf"):
        safe_name = f"{safe_name}.pdf"

    return safe_name


def download_pdf(file_url: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Descargando PDF: %s", file_url)

    response = requests.get(
        file_url,
        timeout=90,
        headers={
            "User-Agent": "RegAlert-DIGEMID-PDFTextExtractor/1.0",
        },
    )
    response.raise_for_status()
    local_path.write_bytes(response.content)

    return local_path


def extract_pages_from_pdf(local_path: Path) -> list[dict]:
    extracted_pages: list[dict] = []

    with fitz.open(local_path) as pdf:
        for page_index, page in enumerate(pdf, start=1):
            text_content = (page.get_text("text") or "").strip()
            extracted_pages.append({
                "page_number": page_index,
                "text_content": text_content,
                "has_text": bool(text_content),
            })

    return extracted_pages


def get_existing_pages(supabase, document_id: str) -> dict[int, str]:
    response = (
        supabase
        .table(PAGE_TABLE_NAME)
        .select("id, page_number")
        .eq("document_id", document_id)
        .execute()
    )

    return {
        int(row["page_number"]): row["id"]
        for row in (response.data or [])
        if row.get("id") and row.get("page_number") is not None
    }


def build_primary_payload(document_id: str, page: dict) -> dict:
    return {
        "document_id": document_id,
        "page_number": page["page_number"],
        "text_content": page["text_content"],
        "extraction_method": TEXT_METHOD,
        "has_text": page["has_text"],
    }


def build_legacy_payload(document_id: str, page: dict) -> dict:
    return {
        "document_id": document_id,
        "page_number": page["page_number"],
        "page_text_raw": page["text_content"],
        "page_text_clean": page["text_content"],
        "raw": {
            "text_content": page["text_content"],
            "extraction_method": TEXT_METHOD,
            "has_text": page["has_text"],
        },
    }


def write_pages(
    supabase,
    document_id: str,
    pages: list[dict],
    payload_builder: Callable[[str, dict], dict],
) -> None:
    existing_pages = get_existing_pages(supabase, document_id)

    for page in pages:
        payload = payload_builder(document_id, page)
        existing_page_id = existing_pages.get(page["page_number"])

        if existing_page_id:
            (
                supabase
                .table(PAGE_TABLE_NAME)
                .update(payload)
                .eq("id", existing_page_id)
                .execute()
            )
            continue

        (
            supabase
            .table(PAGE_TABLE_NAME)
            .insert(payload)
            .execute()
        )


def upsert_document_pages(supabase, document_id: str, pages: list[dict]) -> str:
    try:
        write_pages(supabase, document_id, pages, build_primary_payload)
        return "target"
    except Exception as primary_error:
        logger.warning(
            "La escritura con columnas objetivo fallo para %s. "
            "Se intentara compatibilidad legacy. Error: %s",
            document_id,
            primary_error,
        )

    write_pages(supabase, document_id, pages, build_legacy_payload)
    return "legacy"


def update_document_after_success(supabase, row: dict, storage_mode: str, pages: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    pages_with_text = sum(1 for page in pages if page["has_text"])

    (
        supabase
        .table("digemid_documentos")
        .update({
            "process_status": "text_extracted",
            "process_message": (
                f"Texto extraido por pagina con {TEXT_METHOD}. "
                f"Modo de escritura: {storage_mode}. "
                f"Paginas: {len(pages)}. Con texto: {pages_with_text}."
            ),
            "processed_at": now,
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
            "process_status": "text_extraction_error",
            "process_message": str(error),
            "updated_at": now,
        })
        .eq("id", row["id"])
        .execute()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env()

    supabase = get_supabase()
    rows = get_documents_without_pages(supabase, args.limit)

    logger.info("Documentos encontrados para extraer texto: %s", len(rows))

    processed_count = 0
    skipped_count = 0
    error_count = 0
    temp_dir = Path("tmp") / "pdf_text_extract"

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
            pages = extract_pages_from_pdf(local_path)
            pages_with_text = sum(1 for page in pages if page["has_text"])

            logger.info(
                "Documento %s | paginas: %s | paginas con texto: %s",
                document_key,
                len(pages),
                pages_with_text,
            )

            if args.dry_run:
                logger.info("DRY RUN %s: no se escribira en Supabase", document_key)
                processed_count += 1
                continue

            storage_mode = upsert_document_pages(supabase, document_id, pages)
            update_document_after_success(supabase, row, storage_mode, pages)

            processed_count += 1
            logger.info(
                "Documento procesado correctamente: %s | modo: %s",
                document_key,
                storage_mode,
            )

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
