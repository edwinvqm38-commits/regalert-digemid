"""Vincula digemid_documentos (normativa recien descubierta por el monitor
diario) con su fila real en digemid_normas, usando la capa de identidad
canonica (identidad_normativa.py) en vez de comparar document_key como texto.

Por que hace falta: agent_normative_monitor.py extrae el tipo de norma con un
regex que no tolera tildes ("RESOLUCION" no matchea "Resolución"), asi que
casi toda "Resolución Ministerial/Directoral/Suprema" termina con un
document_key de respaldo tipo NORM-<SECCION>-<ANIO>-<HASH> en vez de uno
limpio ("RM-614-2026"). Esos documentos quedan invisibles para /normarevisar
y para ocr_normativa_openai_pages.py, que buscan por document_key en
digemid_normas -no en digemid_documentos-.

Deliberadamente conservador, mismo principio que resolver_identidad(): "ante
la duda, no vincular".
  - Si el titulo resuelve a una identidad que YA EXISTE en digemid_normas
    (cualquier nivel no ambiguo), se reporta el document_key real para usar
    con /normarevisar. No se escribe nada: ya se puede corregir hoy.
  - Si resuelve a NORMA_NO_ENCONTRADA (no esta en el catalogo), se crea una
    fila nueva en digemid_normas con process_status="inventory_imported",
    para que entre a la MISMA cola que ya procesa
    extract_normativa_text_simple.py. has_file queda en False: el PDF no fue
    verificado (misma cautela que NormativePdfDetectorAgent/F-03B).
  - Si resuelve AMBIGUA o DATOS_INSUFICIENTES, NO se crea nada: se reporta
    para revision humana. Crear una fila igual repetiria el patron de "stub"
    que docs/IDENTIDAD_NORMATIVA_H05_H06_H07.md ya documenta como causa de
    identidades duplicadas (H-08, todavia sin resolver) -por eso este script
    nunca elige "la primera" candidata ni inventa un tipo/anio que el titulo
    no trae.

Uso:
  python scripts/vincular_normativa_identidad.py            # dry-run (default)
  python scripts/vincular_normativa_identidad.py --apply    # crea las normas nuevas
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from agents.agent_utils import deduplicar_por_detalle
from scripts.identidad_normativa import (
    AMBIGUA,
    DATOS_INSUFICIENTES,
    NormaIdentity,
    construir_identidad,
    resolver_identidad,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

NORMAS_TABLE = "digemid_normas"
DOCUMENTOS_TABLE = "digemid_documentos"

# Mismos tipos que TIPOS_CANONICOS en identidad_normativa.py, pero como frase
# para buscar dentro del titulo scrapeado. A diferencia de TYPE_PATTERNS en
# agent_normative_monitor.py, tolera tildes (resoluci[oó]n).
TIPO_TITULO_PATTERN = re.compile(
    r"\b(resoluci[oó]n\s+ministerial|resoluci[oó]n\s+directoral|resoluci[oó]n\s+suprema|"
    r"resoluci[oó]n\s+vicem?inisterial|resoluci[oó]n\s+jefatural|"
    r"resoluci[oó]n\s+de\s+gerencia\s+general|"
    r"decreto\s+supremo|decreto\s+legislativo|decreto\s+de\s+urgencia|ley)\b",
    re.IGNORECASE,
)

NUMERO_TITULO_PATTERN = re.compile(
    r"n[°oº.]*\s*(\d{1,6}(?:[-/]\d{2,4})?(?:[-/][A-Za-zÁÉÍÓÚÑ][\w\-/]*)?)",
    re.IGNORECASE,
)


def identidad_desde_titulo(titulo: str | None) -> NormaIdentity | None:
    """Extrae tipo+numero+anio+sector del titulo scrapeado. Nunca adivina:
    si no hay tipo o numero reconocible, devuelve None en vez de arriesgar
    una identidad incorrecta."""
    if not titulo:
        return None

    match_tipo = TIPO_TITULO_PATTERN.search(titulo)
    if not match_tipo:
        return None

    match_numero = NUMERO_TITULO_PATTERN.search(titulo[match_tipo.end():])
    if not match_numero:
        return None

    identidad = construir_identidad(match_tipo.group(1), match_numero.group(1))
    return identidad if identidad.es_utilizable else None


def document_key_limpio(identidad: NormaIdentity) -> str:
    """Misma convencion que ya usan las filas limpias existentes (ej.
    RD-150-2025, DS-4-2025): tipo-numero[-anio], sin sector."""
    partes = [identidad.tipo, identidad.numero]
    if identidad.anio:
        partes.append(str(identidad.anio))
    return "-".join(partes)


def cargar_catalogo(supabase) -> list[dict]:
    tamano_pagina = 1000
    filas: list[dict] = []
    offset = 0

    while True:
        response = (
            supabase.table(NORMAS_TABLE)
            .select("id, document_key, tipo_norma, numero, anio")
            .range(offset, offset + tamano_pagina - 1)
            .execute()
        )
        lote = response.data or []
        filas.extend(lote)
        if len(lote) < tamano_pagina:
            break
        offset += tamano_pagina

    return filas


def cargar_pendientes(supabase) -> list[dict]:
    response = (
        supabase.table(DOCUMENTOS_TABLE)
        .select("document_key, title, source_section, published_date, detail_url, file_url")
        .eq("source_type", "normativa")
        .execute()
    )
    return deduplicar_por_detalle(response.data or [])


def crear_norma_nueva(supabase, doc: dict, identidad: NormaIdentity, document_key: str) -> None:
    payload = {
        "document_key": document_key,
        "source_type": "norma",
        "source_section": doc.get("source_section"),
        "tipo_norma": identidad.tipo,
        "numero": identidad.numero,
        "anio": identidad.anio,
        "titulo": doc.get("title"),
        "fecha_publicacion": doc.get("published_date"),
        "source_url": doc.get("detail_url"),
        "pdf_url": doc.get("file_url"),
        "mime_type": "application/pdf" if doc.get("file_url") else None,
        "has_file": False,
        "process_status": "inventory_imported",
        "ocr_required": False,
        "has_tables": False,
        "drive_structure": {},
        "botica_relevance": {},
        "derogacion_analizada": False,
        "raw": {
            "puente_identidad_desde_document_key": doc.get("document_key"),
            "puente_identidad_script": "vincular_normativa_identidad.py",
        },
    }
    supabase.table(NORMAS_TABLE).upsert(payload, on_conflict="document_key").execute()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Crea en Supabase las normas nuevas encontradas. Sin esto, solo reporta.",
    )
    args = parser.parse_args()

    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    supabase = create_client(url, key)

    catalogo = cargar_catalogo(supabase)
    pendientes = cargar_pendientes(supabase)

    logger.info(
        "Catalogo digemid_normas: %s fila(s). Documentos normativos a resolver: %s.",
        len(catalogo),
        len(pendientes),
    )

    ya_existentes: list[tuple[str, str]] = []
    creadas: list[tuple[str, str]] = []
    ambiguas: list[str] = []
    sin_datos: list[str] = []

    for doc in pendientes:
        identidad = identidad_desde_titulo(doc.get("title"))
        if identidad is None:
            sin_datos.append(doc["document_key"])
            continue

        resultado = resolver_identidad(identidad, catalogo)

        if resultado.resuelta:
            ya_existentes.append((doc["document_key"], resultado.norma["document_key"]))
            continue

        if resultado.nivel == AMBIGUA:
            candidatas = ", ".join(c["document_key"] for c in resultado.candidatas)
            ambiguas.append(f"{doc['document_key']} ({identidad}) -> candidatas: {candidatas}")
            continue

        if resultado.nivel == DATOS_INSUFICIENTES:
            sin_datos.append(doc["document_key"])
            continue

        # NORMA_NO_ENCONTRADA: genuinamente nueva.
        nueva_key = document_key_limpio(identidad)
        creadas.append((doc["document_key"], nueva_key))

        if args.apply:
            crear_norma_nueva(supabase, doc, identidad, nueva_key)

        # Se agrega al catalogo en memoria (se haya aplicado o no) para que un
        # segundo documento del mismo lote con la misma identidad (ej. la
        # misma norma listada en dos secciones, aunque deduplicar_por_detalle
        # ya cubre el caso de mismo detail_url) resuelva contra esta entrada
        # en vez de reportarse tambien como "nueva".
        catalogo.append({
            "id": None,
            "document_key": nueva_key,
            "tipo_norma": identidad.tipo,
            "numero": identidad.numero,
            "anio": identidad.anio,
        })

    logger.info(
        "Ya existian en digemid_normas -usar ese document_key con /normarevisar-: %s",
        len(ya_existentes),
    )
    for viejo, real in ya_existentes:
        logger.info("  %s -> %s", viejo, real)

    logger.info(
        "%s normas nuevas (--apply=%s): %s",
        "Creadas" if args.apply else "Se crearian",
        args.apply,
        len(creadas),
    )
    for viejo, nueva in creadas:
        logger.info("  %s -> %s", viejo, nueva)

    logger.info("Identidad ambigua -necesitan revision humana-: %s", len(ambiguas))
    for linea in ambiguas:
        logger.info("  %s", linea)

    logger.info("Sin tipo/numero reconocible en el titulo: %s", len(sin_datos))
    for key_pendiente in sin_datos:
        logger.info("  %s", key_pendiente)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logger.exception("Error critico vinculando identidad normativa: %s", error)
        sys.exit(1)
