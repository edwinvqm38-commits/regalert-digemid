"""Backfill: genera y sube la imagen PNG de las páginas de baja calidad que ya
existen en digemid_norma_paginas pero fueron insertadas antes de que la
ingesta empezara a generar page_image_storage_path (ver
scripts/extract_normativa_text_simple.py).

Sin esto, las normas ya procesadas (p.ej. RM-607-2024) seguirían mostrando el
reporte de revisión sin la imagen real de la página -- solo las normas
procesadas de aquí en adelante la tendrían.

Uso:
    # Una norma puntual
    python scripts/generar_imagenes_paginas_baja_calidad.py --document-key RM-607-2024

    # Todas las normas con páginas de baja calidad pendientes de imagen
    python scripts/generar_imagenes_paginas_baja_calidad.py --all
"""

import argparse
import logging
from pathlib import Path

import fitz
import requests
from dotenv import load_dotenv
from supabase import create_client

import os


NORMA_TABLE = "digemid_normas"
PAGE_TABLE = "digemid_norma_paginas"
STORAGE_BUCKET = "digemid-documentos"
UMBRAL_BAJA_CALIDAD = 0.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument("--document-key", help="Procesar una sola norma")
    grupo.add_argument(
        "--all", action="store_true", help="Procesar todas las normas con paginas pendientes de imagen"
    )
    return parser.parse_args()


def get_normas_con_paginas_pendientes(supabase, document_key: str | None) -> list[dict]:
    query = (
        supabase.table(PAGE_TABLE)
        .select("norma_id")
        .lt("quality_score", UMBRAL_BAJA_CALIDAD)
        .is_("page_image_storage_path", "null")
    )
    rows = query.execute().data or []
    norma_ids = sorted({row["norma_id"] for row in rows})
    if not norma_ids:
        return []

    normas_query = supabase.table(NORMA_TABLE).select(
        "id, document_key, titulo, pdf_url, file_storage_path"
    )
    if document_key:
        normas_query = normas_query.eq("document_key", document_key)
    else:
        normas_query = normas_query.in_("id", norma_ids)

    normas = normas_query.execute().data or []
    norma_ids_pendientes = set(norma_ids)
    return [n for n in normas if n["id"] in norma_ids_pendientes]


def get_paginas_pendientes(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table(PAGE_TABLE)
        .select("id, page_number, quality_score")
        .eq("norma_id", norma_id)
        .lt("quality_score", UMBRAL_BAJA_CALIDAD)
        .is_("page_image_storage_path", "null")
        .execute()
    )
    return response.data or []


def download_pdf_bytes(supabase, norma: dict) -> bytes:
    storage_path = (norma.get("file_storage_path") or "").strip()
    if storage_path:
        data = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)
        if data and data.startswith(b"%PDF"):
            return data
        logger.warning("Storage no devolvio PDF valido en %s, intento con pdf_url.", storage_path)

    pdf_url = (norma.get("pdf_url") or "").strip()
    if not pdf_url:
        raise ValueError(f"{norma.get('document_key')} no tiene file_storage_path ni pdf_url.")
    response = requests.get(
        pdf_url, timeout=120, headers={"User-Agent": "RegAlert-DIGEMID-BackfillImagenes/1.0"}
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise ValueError(f"URL no devolvio PDF valido: {pdf_url}")
    return response.content


def procesar_norma(supabase, norma: dict) -> tuple[int, int]:
    document_key = norma["document_key"]
    paginas = get_paginas_pendientes(supabase, norma["id"])
    if not paginas:
        return (0, 0)

    pdf_bytes = download_pdf_bytes(supabase, norma)
    subidas = 0
    fallidas = 0

    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for pagina in paginas:
            page_number = pagina["page_number"]
            try:
                if page_number < 1 or page_number > len(doc):
                    raise ValueError(f"el PDF tiene {len(doc)} paginas, no existe la pagina {page_number}")
                page = doc[page_number - 1]
                pix = page.get_pixmap(matrix=fitz.Matrix(150 / 72, 150 / 72), alpha=False)
                png_bytes = pix.tobytes("png")
                object_path = f"paginas-baja-calidad/{document_key}/pagina-{page_number}.png"
                supabase.storage.from_(STORAGE_BUCKET).upload(
                    object_path,
                    png_bytes,
                    file_options={"content-type": "image/png", "upsert": "true"},
                )
                supabase.table(PAGE_TABLE).update({"page_image_storage_path": object_path}).eq(
                    "id", pagina["id"]
                ).execute()
                subidas += 1
                logger.info("%s pagina %s: imagen subida a %s", document_key, page_number, object_path)
            except Exception:
                fallidas += 1
                logger.exception("%s pagina %s: fallo al generar/subir imagen", document_key, page_number)

    return (subidas, fallidas)


def main():
    args = parse_args()
    load_env()
    supabase = get_supabase()

    normas = get_normas_con_paginas_pendientes(supabase, args.document_key)
    if not normas:
        logger.info("No hay paginas de baja calidad pendientes de imagen.")
        return

    total_subidas = 0
    total_fallidas = 0
    for norma in normas:
        try:
            subidas, fallidas = procesar_norma(supabase, norma)
        except Exception:
            logger.exception("No se pudo procesar %s", norma.get("document_key"))
            continue
        total_subidas += subidas
        total_fallidas += fallidas

    logger.info(
        "Listo. %s norma(s) procesada(s), %s imagen(es) subida(s), %s fallida(s).",
        len(normas),
        total_subidas,
        total_fallidas,
    )


if __name__ == "__main__":
    main()
