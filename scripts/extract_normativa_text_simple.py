"""Extrae texto de PDFs de normativa DIGEMID directo desde digemid.minsa.gob.pe
(sin Google Drive) y lo guarda en digemid_norma_paginas.

Solo procesa normas que tengan pdf_url directo. Las normas sin pdf_url
requieren un paso previo de rastreo de su pagina oficial (no cubierto aqui).
"""

import argparse
import logging
import os
import time
import unicodedata
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

PAGE_TABLE = "digemid_norma_paginas"
NORMAS_TABLE = "digemid_normas"
EXTRACTION_METHOD = "pymupdf_directo"
DELAY_BETWEEN_DESCARGAS_SEGUNDOS = 2.0
MAX_REINTENTOS_429 = 3


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def get_pending_normas(supabase, limit: int) -> list[dict]:
    response = (
        supabase.table(NORMAS_TABLE)
        .select("id, document_key, pdf_url, file_name")
        .not_.is_("pdf_url", "null")
        .neq("pdf_url", "")
        .order("anio", desc=True)
        .limit(limit)
        .execute()
    )
    normas = response.data or []
    if not normas:
        return []

    norma_ids = [n["id"] for n in normas]
    paginas = (
        supabase.table(PAGE_TABLE)
        .select("norma_id")
        .in_("norma_id", norma_ids)
        .execute()
    )
    con_paginas = {row["norma_id"] for row in (paginas.data or []) if row.get("norma_id")}

    return [n for n in normas if n["id"] not in con_paginas]


def sanitize_file_name(document_key: str, file_name: str | None) -> str:
    base = file_name or f"{document_key}.pdf"
    safe = base.replace("/", "-").replace("\\", "-").strip()
    if not safe.lower().endswith(".pdf"):
        safe = f"{safe}.pdf"
    return safe


def download_pdf(url: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Descargando PDF normativo: %s", url)

    for intento in range(1, MAX_REINTENTOS_429 + 1):
        response = requests.get(
            url,
            timeout=90,
            headers={"User-Agent": "RegAlert-DIGEMID-NormativaText/1.0"},
        )
        if response.status_code == 429 and intento < MAX_REINTENTOS_429:
            espera = float(response.headers.get("Retry-After", 10 * intento))
            logger.warning("429 en %s (intento %s). Espero %.1fs.", url, intento, espera)
            time.sleep(espera)
            continue
        response.raise_for_status()
        local_path.write_bytes(response.content)
        return local_path

    raise RuntimeError(f"No se pudo descargar {url} tras {MAX_REINTENTOS_429} intentos (429)")


def normalize_text(text: str) -> str:
    return unicodedata.normalize("NFC", text).strip()


def extract_pages(local_path: Path) -> list[dict]:
    pages: list[dict] = []
    with fitz.open(local_path) as pdf:
        for page_index, page in enumerate(pdf, start=1):
            raw = page.get_text("text") or ""
            pages.append({
                "page_number": page_index,
                "text_raw": raw,
                "text_normalized": normalize_text(raw),
            })
    return pages


def write_pages(supabase, norma_id: str, pages: list[dict]) -> None:
    for page in pages:
        payload = {
            "norma_id": norma_id,
            "page_number": page["page_number"],
            "text_raw": page["text_raw"],
            "text_normalized": page["text_normalized"],
            "extraction_method": EXTRACTION_METHOD,
            "ocr_used": False,
        }
        supabase.table(PAGE_TABLE).insert(payload).execute()


def mark_norma(supabase, norma_id: str, status: str, message: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    supabase.table(NORMAS_TABLE).update(
        {"process_status": status, "updated_at": now}
    ).eq("id", norma_id).execute()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env()
    supabase = get_supabase()

    normas = get_pending_normas(supabase, args.limit)
    logger.info("Normas con pdf_url pendientes de texto: %s", len(normas))

    procesadas = 0
    errores = 0
    temp_dir = Path("tmp") / "normativa_text"

    for index, norma in enumerate(normas):
        if index > 0:
            time.sleep(DELAY_BETWEEN_DESCARGAS_SEGUNDOS)

        document_key = norma["document_key"]
        pdf_url = norma["pdf_url"]
        file_name = sanitize_file_name(document_key, norma.get("file_name"))

        try:
            if args.dry_run:
                logger.info("[dry-run] Extraeria texto de %s (%s)", document_key, pdf_url)
                procesadas += 1
                continue

            local_path = temp_dir / file_name
            download_pdf(pdf_url, local_path)
            pages = extract_pages(local_path)
            con_texto = sum(1 for p in pages if p["text_normalized"])

            write_pages(supabase, norma["id"], pages)
            mark_norma(
                supabase,
                norma["id"],
                "text_extracted",
                f"{len(pages)} paginas, {con_texto} con texto ({EXTRACTION_METHOD}).",
            )
            procesadas += 1
            logger.info("%s | paginas: %s | con texto: %s", document_key, len(pages), con_texto)

        except Exception as error:
            errores += 1
            logger.exception("Error procesando %s: %s", document_key, error)
            if not args.dry_run:
                mark_norma(supabase, norma["id"], "text_extraction_error", str(error))

    logger.info("Finalizado. Procesadas: %s | Errores: %s", procesadas, errores)


if __name__ == "__main__":
    main()
