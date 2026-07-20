"""Descubre el pdf_url de normas DIGEMID que solo tienen source_url (su pagina
oficial). Visita cada pagina, encuentra el enlace al PDF oficial y lo guarda en
digemid_normas.pdf_url para que el extractor de texto pueda procesarlas.

Pensado para correr en lotes pequenos (ej. 20/dia) con pausas largas, para no
sobrecargar ni ser bloqueados por el servidor de DIGEMID.
"""

import argparse
import logging
import os
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

NORMAS_TABLE = "digemid_normas"
DELAY_SEGUNDOS = 4.0
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


def get_normas_sin_pdf(supabase, limit: int, document_key: str | None = None) -> list[dict]:
    query = (
        supabase.table(NORMAS_TABLE)
        .select("id, document_key, source_url")
        .not_.is_("source_url", "null")
        .neq("source_url", "")
    )
    if document_key:
        query = query.eq("document_key", document_key)
    else:
        query = query.or_("pdf_url.is.null,pdf_url.eq.").order("anio", desc=True)

    response = query.limit(limit).execute()
    return response.data or []


BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    "Referer": "https://www.digemid.minsa.gob.pe/",
    "Upgrade-Insecure-Requests": "1",
}


def fetch_html(url: str) -> str | None:
    for intento in range(1, MAX_REINTENTOS_429 + 1):
        try:
            response = requests.get(url, timeout=60, headers=BROWSER_HEADERS)
        except Exception as error:
            logger.warning("Error de red en %s: %s", url, error)
            return None

        if response.status_code == 429 and intento < MAX_REINTENTOS_429:
            espera = float(response.headers.get("Retry-After", 10 * intento))
            logger.warning("429 en %s (intento %s). Espero %.1fs.", url, intento, espera)
            time.sleep(espera)
            continue

        if not response.ok:
            logger.warning("HTTP %s en %s", response.status_code, url)
            return None

        return response.text

    return None


def elegir_pdf(html: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    candidatos: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            candidatos.append(urljoin(base_url, href))

    if not candidatos:
        return None

    # Preferir PDFs del repositorio oficial de normatividad.
    def prioridad(u: str) -> int:
        low = u.lower()
        if "archivos/normatividad" in low:
            return 0
        if "archivos" in low:
            return 1
        return 2

    candidatos.sort(key=prioridad)
    return candidatos[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--document-key", default=None,
                        help="Rastrear SOLO esta norma (busca su PDF aunque ya tenga pdf_url).")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env()
    supabase = get_supabase()

    normas = get_normas_sin_pdf(supabase, args.limit, args.document_key)
    logger.info("Normas sin pdf_url a rastrear: %s", len(normas))

    encontrados = 0
    sin_pdf = 0
    errores = 0

    for index, norma in enumerate(normas):
        if index > 0:
            time.sleep(DELAY_SEGUNDOS)

        document_key = norma["document_key"]
        source_url = norma["source_url"]

        try:
            html = fetch_html(source_url)
            if not html:
                errores += 1
                continue

            pdf_url = elegir_pdf(html, source_url)
            if not pdf_url:
                sin_pdf += 1
                logger.info("%s: no se encontro PDF en %s", document_key, source_url)
                continue

            logger.info("%s -> %s", document_key, pdf_url)
            if not args.dry_run:
                supabase.table(NORMAS_TABLE).update({"pdf_url": pdf_url}).eq("id", norma["id"]).execute()
            encontrados += 1

        except Exception as error:
            errores += 1
            logger.exception("Error rastreando %s: %s", document_key, error)

    logger.info(
        "Finalizado. PDF encontrados: %s | Sin PDF: %s | Errores: %s",
        encontrados, sin_pdf, errores,
    )


if __name__ == "__main__":
    main()
