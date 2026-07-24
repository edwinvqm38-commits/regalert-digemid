"""Corrige un falso positivo del pipeline de calidad: paginas GENUINAMENTE en
blanco en el PDF original que quedaron marcadas como "baja confiabilidad"
(quality_score < 0.5) por el mismo motivo que una pagina con contenido real
que fallo al transcribirse — ambas dan menos de 15 caracteres de texto.

Este script vuelve a mirar cada pagina ya marcada y, si el render de esa
pagina en el PDF es realmente blanco (sin texto ni imagenes), la corrige en
Supabase (quality_score=1, extraction_method='pagina_en_blanco') para que
deje de aparecer en /normasrevisar. Las paginas que SI tienen contenido real
pero se transcribieron mal se quedan en la cola, sin tocar.

De aqui en adelante agents/pdf_extract.py ya detecta esto de entrada, asi que
este script es para limpiar el atraso que quedo de antes del fix.
"""

import logging
import os
import sys
import tempfile
from pathlib import Path

import fitz  # PyMuPDF
import requests
from dotenv import load_dotenv
from supabase import create_client

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from agents.pdf_extract import es_pagina_en_blanco

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PAGE_TABLE = "digemid_norma_paginas"
NORMAS_TABLE = "digemid_normas"
STORAGE_BUCKET = "digemid-documentos"
UMBRAL_BAJA_CALIDAD = 0.5


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def get_paginas_candidatas(supabase) -> list[dict]:
    response = (
        supabase.table(PAGE_TABLE)
        .select("id, norma_id, page_number")
        .lt("quality_score", UMBRAL_BAJA_CALIDAD)
        .eq("revisado_manual", False)
        .execute()
    )
    return response.data or []


def descargar_pdf(norma: dict, destino: Path) -> bool:
    file_storage_path = norma.get("file_storage_path")
    pdf_url = norma.get("pdf_url")

    if file_storage_path:
        try:
            supabase = get_supabase()
            contenido = supabase.storage.from_(STORAGE_BUCKET).download(file_storage_path)
            destino.write_bytes(contenido)
            return True
        except Exception:
            logger.warning("No se pudo descargar %s desde Storage, probando pdf_url.", file_storage_path)

    if pdf_url:
        response = requests.get(pdf_url, timeout=120, headers={"User-Agent": "RegAlert-DIGEMID-BlancoFix/1.0"})
        if response.ok:
            destino.write_bytes(response.content)
            return True

    return False


def main():
    load_env()
    supabase = get_supabase()

    candidatas = get_paginas_candidatas(supabase)
    logger.info("Paginas candidatas (baja calidad, no revisadas): %s", len(candidatas))

    if not candidatas:
        print("Sin paginas candidatas.")
        return

    por_norma: dict[str, list[dict]] = {}
    for fila in candidatas:
        por_norma.setdefault(fila["norma_id"], []).append(fila)

    total_en_blanco = 0
    total_pendiente = 0
    total_error = 0

    with tempfile.TemporaryDirectory() as tmp:
        for norma_id, filas in por_norma.items():
            norma_resp = (
                supabase.table(NORMAS_TABLE)
                .select("id, document_key, pdf_url, file_storage_path")
                .eq("id", norma_id)
                .maybe_single()
                .execute()
            )
            norma = norma_resp.data
            if not norma:
                total_error += len(filas)
                continue

            pdf_path = Path(tmp) / f"{norma['document_key']}.pdf"
            if not descargar_pdf(norma, pdf_path):
                logger.warning("No se pudo descargar el PDF de %s.", norma["document_key"])
                total_error += len(filas)
                continue

            try:
                doc = fitz.open(str(pdf_path))
            except Exception:
                logger.exception("No se pudo abrir el PDF de %s.", norma["document_key"])
                total_error += len(filas)
                continue

            for fila in filas:
                page_number = fila["page_number"]
                page_index = page_number - 1

                if page_index < 0 or page_index >= len(doc):
                    total_error += 1
                    continue

                if es_pagina_en_blanco(doc[page_index]):
                    supabase.table(PAGE_TABLE).update({
                        "text_raw": "",
                        "text_normalized": "",
                        "quality_score": 1,
                        "extraction_method": "pagina_en_blanco",
                        "ocr_used": False,
                    }).eq("id", fila["id"]).execute()
                    total_en_blanco += 1
                    logger.info("%s pagina %s: en blanco, corregida.", norma["document_key"], page_number)
                else:
                    total_pendiente += 1

            doc.close()

    print(
        f"Revisadas: {len(candidatas)} | En blanco (corregidas): {total_en_blanco} | "
        f"Con contenido real, siguen pendientes: {total_pendiente} | Errores: {total_error}"
    )


if __name__ == "__main__":
    main()
