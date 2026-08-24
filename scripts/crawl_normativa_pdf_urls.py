"""Descubre el pdf_url de normas DIGEMID que solo tienen source_url (su pagina
oficial), COMPROBANDO que el PDF sea realmente el de esa norma.

Historia de por que esto importa (F-03):

    La version anterior de elegir_pdf() recogia todos los enlaces .pdf de la
    pagina, los ordenaba por su ruta y devolvia `candidatos[0]`. Nunca recibia
    la norma objetivo, asi que era estructuralmente incapaz de comprobar si el
    documento elegido era el correcto. Entre el 2026-07-23 y el 2026-08-24
    corrio cada hora en modo apply, y asigno a varias normas el PDF de OTRA:
    LEY-29698 quedo apuntando a RM_373-2024-MINSA.pdf, RM-1000-2016 y
    RM-1001-2016 quedaron intercambiadas, RM-734-2025 apunta a su ANEXO.

Ahora:

    UN PDF ENCONTRADO NO ES UN PDF IDENTIFICADO.

Se descarga cada candidato, se leen sus encabezados normativos y se compara
con la identidad de la norma. Solo se escribe pdf_url cuando el documento
DEMUESTRA contener la norma. Ante ambiguedad, contradiccion o falta de prueba
no se escribe nada: el caso queda registrado para revision.

Pensado para correr en lotes pequenos con pausas largas, para no sobrecargar
ni ser bloqueados por el servidor de DIGEMID.
"""

import argparse
import hashlib
import logging
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "agents"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from identidad_documental import (  # noqa: E402
    EvidenciaDocumental,
    identidades_en_texto,
    resolver_pdf_para_norma,
)
from identidad_normativa import construir_identidad  # noqa: E402

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
        .select("id, document_key, source_url, tipo_norma, numero, anio")
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


def es_nombre_archivo_valido(url: str) -> bool:
    # Algunas paginas de DIGEMID traen un enlace roto/plantilla que termina
    # en "/.pdf" (nombre de archivo vacio) — no es un PDF real, se descarta.
    nombre_archivo = url.split("?", 1)[0].rsplit("/", 1)[-1]
    return nombre_archivo != ".pdf" and len(nombre_archivo) > len(".pdf")


# Techo de seguridad frente a una pagina patologica, NO un recorte silencioso.
# Cuando se alcanza, el numero de candidatos omitidos viaja hasta el resolvedor
# y el resultado es AUDITORIA_INCOMPLETA: "no los mire todos" nunca se degrada
# a "la norma no esta en ninguno".
MAX_CANDIDATOS = 40
SEGUNDOS_POR_CANDIDATO = 60.0


def candidatos_de_pdf(html: str, base_url: str) -> tuple[list[tuple[str, str]], int]:
    """Todos los enlaces a PDF de la pagina, con su texto de enlace.

    Ya NO se ordena por ruta ni se devuelve el primero: la ruta no dice a que
    norma pertenece un documento. Se devuelven todos y decide la evidencia.
    """
    soup = BeautifulSoup(html, "html.parser")
    candidatos: list[tuple[str, str]] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if ".pdf" not in href.lower():
            continue
        url = urljoin(base_url, href)
        if es_nombre_archivo_valido(url):
            candidatos.append((url, a.get_text(" ", strip=True)))

    if not candidatos:
        # Respaldo: PDFs embebidos en bloques de datos (visores, JSON de un
        # framework). Sin texto de enlace, asi que la evidencia sera solo el
        # contenido del documento.
        for match in re.finditer(
            r'https?://[^\s"\'<>\\]+\.pdf(?:\?[^\s"\'<>\\]*)?', html, re.IGNORECASE
        ):
            url = match.group(0)
            if es_nombre_archivo_valido(url):
                candidatos.append((url, ""))

    vistos, unicos = set(), []
    for url, texto in candidatos:
        if url not in vistos:
            vistos.add(url)
            unicos.append((url, texto))
    omitidos = max(0, len(unicos) - MAX_CANDIDATOS)
    if omitidos:
        logger.warning(
            "%s: la pagina lista %d PDF y el techo es %d; %d quedan sin auditar "
            "-> el resultado sera AUDITORIA_INCOMPLETA",
            base_url, len(unicos), MAX_CANDIDATOS, omitidos,
        )
    return unicos[:MAX_CANDIDATOS], omitidos


def evidencia_de_candidato(url: str, anchor_text: str, identidad_objetivo) -> EvidenciaDocumental:
    """Descarga el PDF y lee sus encabezados. El CONTENIDO es la evidencia
    principal; el nombre del archivo y el texto del enlace solo acompañan."""
    ev = EvidenciaDocumental(
        identidad_objetivo=identidad_objetivo,
        filename=url.rsplit("/", 1)[-1],
        anchor_text=anchor_text,
        url=url,
    )
    try:
        respuesta = requests.get(url, timeout=90, headers=BROWSER_HEADERS)
        respuesta.raise_for_status()
        import io

        import fitz

        ev.pdf_sha256 = hashlib.sha256(respuesta.content).hexdigest()

        with fitz.open(stream=io.BytesIO(respuesta.content), filetype="pdf") as doc:
            ev.total_paginas = doc.page_count
            textos = []
            leidas = 0
            arranque = time.monotonic()
            # TODAS las paginas: el encabezado puede estar en cualquiera. En una
            # edicion de El Peruano la norma buscada suele estar en el medio, y
            # cortar en la pagina 12 la haria invisible.
            for indice in range(doc.page_count):
                if time.monotonic() - arranque > SEGUNDOS_POR_CANDIDATO:
                    ev.motivo_incompletitud = (
                        f"se agoto el presupuesto de {SEGUNDOS_POR_CANDIDATO:.0f}s "
                        f"tras {leidas} de {doc.page_count} paginas"
                    )
                    break
                texto = doc[indice].get_text("text") or ""
                textos.append(texto)
                ev.apariciones.extend(identidades_en_texto(texto, indice + 1))
                leidas += 1
            ev.paginas_analizadas = leidas
            ev.texto_completo = "\n".join(textos)
    except Exception as error:
        logger.warning("No se pudo leer el candidato %s: %s", url, error)
        ev.pdf_disponible = False
    return ev


def resolver_pdf(html: str, base_url: str, norma: dict):
    """Devuelve el ResultadoResolucion para la norma dada. Puede no elegir
    ninguno: eso es un resultado valido y correcto."""
    identidad = construir_identidad(norma.get("tipo_norma"), norma.get("numero"), norma.get("anio"))
    candidatos, omitidos = candidatos_de_pdf(html, base_url)
    if not candidatos:
        return None, identidad

    evidencias = []
    for url, anchor_text in candidatos:
        evidencias.append(evidencia_de_candidato(url, anchor_text, identidad))
        time.sleep(1.0)   # cortesia con el servidor

    return resolver_pdf_para_norma(
        evidencias, identidad,
        candidatos_omitidos=omitidos,
        motivo_omision=(f"la pagina listaba mas de {MAX_CANDIDATOS} PDF" if omitidos else ""),
    ), identidad


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
    no_verificados = 0
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

            resultado, identidad = resolver_pdf(html, source_url, norma)
            if resultado is None:
                sin_pdf += 1
                logger.info("%s: no se encontro ningun PDF en %s", document_key, source_url)
                continue

            if not resultado.puede_escribirse:
                # AMBIGUO, CONTRADICTORIO o NO_ENCONTRADO: no se escribe nada.
                # Asignar un PDF sin prueba es exactamente lo que dejo normas
                # con la transcripcion de otra norma.
                no_verificados += 1
                logger.warning(
                    "%s (%s): NO se escribe pdf_url — %s: %s",
                    document_key, identidad, resultado.estado, resultado.motivo,
                )
                for candidato in resultado.candidatos_evaluados:
                    logger.info("    candidato %s -> %s (%s)",
                                candidato["url"], candidato["clasificacion"], candidato["motivo"])
                continue

            logger.info("%s -> %s [%s, paginas %s-%s]", document_key, resultado.url,
                        resultado.estado, resultado.start_page, resultado.end_page)
            if not args.dry_run:
                supabase.table(NORMAS_TABLE).update(
                    {"pdf_url": resultado.url}
                ).eq("id", norma["id"]).execute()
            encontrados += 1

        except Exception as error:
            errores += 1
            logger.exception("Error rastreando %s: %s", document_key, error)

    logger.info(
        "Finalizado. PDF verificados y escritos: %s | Sin PDF: %s | "
        "Hallados pero NO verificados (no escritos): %s | Errores: %s",
        encontrados, sin_pdf, no_verificados, errores,
    )


if __name__ == "__main__":
    main()
