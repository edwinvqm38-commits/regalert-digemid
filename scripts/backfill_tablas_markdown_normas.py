"""Corrige el atraso de paginas de normativa extraidas ANTES de que
extract_normativa_text_simple.py empezara a anexar las tablas detectadas
(page.tables, via pdfplumber) como markdown al final de text_normalized.

Esas paginas ya tienen la tabla guardada como estructura en
metadata->tables (nunca se perdio), pero text_normalized solo tiene el
texto aplanado por PyMuPDF/pdfplumber, donde una fila como
"Item / Infraccion / Condicion / Referencia legal" no deja claro que celda
es de que columna. Este script relee esa estructura ya guardada y la anexa,
sin tener que re-descargar ni re-OCRear el PDF.

Idempotente: si text_normalized ya trae el marcador "Tabla detectada:" no
la vuelve a anexar.
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from agents.pdf_extract import tablas_a_texto

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PAGE_TABLE = "digemid_norma_paginas"
MARCADOR = "Tabla detectada:"


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def get_paginas_pendientes(supabase, limit: int) -> list[dict]:
    response = (
        supabase.table(PAGE_TABLE)
        .select("id, text_normalized, metadata")
        .eq("has_tables", True)
        .not_.like("text_normalized", f"%{MARCADOR}%")
        .limit(limit)
        .execute()
    )
    return response.data or []


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env()
    supabase = get_supabase()

    paginas = get_paginas_pendientes(supabase, args.limit)
    logger.info("Paginas con tabla detectada pendientes de formatear: %s", len(paginas))

    actualizadas = 0
    sin_tablas_utiles = 0

    for pagina in paginas:
        tablas = (pagina.get("metadata") or {}).get("tables")
        bloque = tablas_a_texto(tablas)

        if not bloque:
            # has_tables=true pero metadata.tables vacio/no aprovechable:
            # no hay nada que anexar.
            sin_tablas_utiles += 1
            continue

        nuevo_texto = f"{(pagina.get('text_normalized') or '').strip()}\n\n{bloque}"

        if args.dry_run:
            logger.info("[dry-run] Anexaria tabla markdown a pagina %s", pagina["id"])
            actualizadas += 1
            continue

        supabase.table(PAGE_TABLE).update(
            {"text_normalized": nuevo_texto}
        ).eq("id", pagina["id"]).execute()
        actualizadas += 1

    logger.info(
        "Finalizado. Paginas actualizadas: %s | sin tabla aprovechable: %s",
        actualizadas, sin_tablas_utiles,
    )


if __name__ == "__main__":
    main()
