import hashlib
import io
import logging
import os
import random
import re
import sys
import time
import unicodedata
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from supabase import Client, create_client

from agents.agent_utils import clean_text, utc_now_iso

# F-03B: este agente escribia pdf_url/file_url tomando `candidate_links[0]`
# tras ordenar por score. El score solo mide que tan prometedor SE VE un
# enlace -nunca abrio el PDF ni lo comparo con la norma objetivo-, asi que un
# candidato de score maximo podia ganar sin que su contenido fuera el
# correcto. Ahora reutiliza la misma politica documental canonica que el
# crawler (scripts/crawl_normativa_pdf_urls.py): abrir cada candidato, leer
# sus encabezados, y escribir SOLO con MATCH_EXACTO o MATCH_MULTINORMA
# probado por contenido. El score sigue existiendo, pero solo para filtrar
# enlaces obviamente irrelevantes y para decidir cual inspeccionar primero si
# hay que recortar por el techo de seguridad -nunca para elegir cual escribir-.
for _extra_path in (Path(__file__).resolve().parent, Path(__file__).resolve().parents[1] / "scripts"):
    if str(_extra_path) not in sys.path:
        sys.path.insert(0, str(_extra_path))

from identidad_documental import (  # noqa: E402
    AMBIGUO,
    AUDITORIA_INCOMPLETA,
    NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA,
    PATRON_ENCABEZADO,
    EvidenciaDocumental,
    identidades_en_texto,
    normalizar_tipo_norma,
)
from identidad_normativa import construir_identidad  # noqa: E402
from politica_documental import REQUIERE_HUMANO, decidir  # noqa: E402

logger = logging.getLogger(__name__)

# Techo de seguridad frente a una pagina patologica, NO un recorte silencioso:
# ver candidatos_de_pdf() en scripts/crawl_normativa_pdf_urls.py, mismo criterio.
MAX_CANDIDATOS = 40
SEGUNDOS_POR_CANDIDATO = 60.0

# A que `process_status` se traduce cada estado del resolvedor cuando NO
# autoriza a escribir. Cualquier estado no listado aqui (no deberia ocurrir)
# cae en "pdf_ambiguous" por seguridad: nunca en "pdf_detected".
_ESTADO_A_STATUS = {
    AMBIGUO: "pdf_ambiguous",
    AUDITORIA_INCOMPLETA: "pdf_audit_incomplete",
    NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA: "pdf_not_found",
    REQUIERE_HUMANO: "pdf_ambiguous",
}

PDF_ANCHOR_HINTS = (
    "descargar",
    "pdf",
    "ver documento",
    "archivo",
)
PDF_PATH_HINTS = (
    "/archivos/normatividad/",
    "/archivos/",
)


def is_pdf_url(url: str | None) -> bool:
    if not url:
        return False
    return ".pdf" in url.lower()


def extract_file_name(file_url: str | None) -> str | None:
    if not file_url:
        return None
    path = urlparse(file_url).path
    file_name = path.rsplit("/", 1)[-1]
    return file_name or None


def _sin_acentos(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn"
    )


def identidad_objetivo_de_documento(title, document_key=None):
    """Identidad objetivo a partir de lo que YA se registro de la norma.

    `digemid_documentos` no tiene columnas propias `tipo_norma`/`numero`/
    `anio` -a diferencia de `digemid_normas`-, solo `document_key` y `title`.
    Y `document_key` con frecuencia es un hash sin estructura
    ("NORM-RESOLUCION-MINISTERIAL-2025-3EEE0B42"), asi que no sirve como
    identidad. El `title` si trae la forma de un encabezado normativo
    ("Resolucion Ministerial N 793-2025/MINSA"), asi que se reutiliza el
    mismo patron que lee encabezados dentro de un PDF.

    Solo se acepta la identidad que aparece AL INICIO del texto (`match`,
    anclado en la posicion 0 -tras quitar acentos y espacios sobrantes-, no
    `finditer` sobre cualquier parte). Un titulo peruano que tiene numero
    propio SIEMPRE lo declara primero: "Resolucion Ministerial N 793-2025
    /MINSA que modifica la Resolucion Ministerial N 419-2025/MINSA" empieza
    declarando la 793 -la cita a la 419 viene despues, en la prosa-.

    La version anterior usaba `identidades_en_texto()` y tomaba la PRIMERA
    aparicion sin importar su posicion. Eso funcionaba por casualidad cuando
    el propio numero encabezaba el titulo, pero un titulo que SOLO cita a
    otra norma sin declarar el suyo -"Modifican la Resolucion Ministerial
    N 419-2025/MINSA", o "Resolucion Ministerial que aprueba el TUPA,
    modificado por Resolucion Ministerial N 100-2020/MINSA"- no tiene ningun
    numero propio en el texto, y la version anterior adoptaba el numero de
    la norma CITADA como si fuera la identidad de ESTE documento. Es
    exactamente la confusion que F-03 existe para evitar, aplicada a la
    fuente del titulo en vez de al contenido del PDF.

    Devuelve `None` cuando no se puede construir una identidad usable -sea
    porque el texto no dice nada, o porque solo cita a otra norma sin
    declarar la suya-: sin identidad objetivo no hay nada que comprobar, y
    sin comprobacion no se escribe (F-03B).
    """
    for fuente in (title, document_key):
        if not fuente:
            continue
        plano = _sin_acentos(str(fuente)).strip()
        coincidencia = PATRON_ENCABEZADO.match(plano)
        if not coincidencia:
            continue
        tipo = normalizar_tipo_norma(coincidencia.group("tipo"))
        if not tipo:
            continue
        numero = re.sub(r"\s+", "", coincidencia.group("numero"))
        identidad = construir_identidad(tipo, numero)
        if identidad.numero:
            return identidad
    return None


class NormativePdfDetectorAgent:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not key:
            raise ValueError(
                "Faltan variables de entorno SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY"
            )

        self.supabase: Client = create_client(url, key)
        self.table_name = "digemid_documentos"
        self.session = requests.Session()
        # El User-Agent generico ("Mozilla/5.0" a secas) y la falta de Referer
        # hacian que el WAF de DIGEMID devolviera 403 en el 100% de los
        # pedidos (confirmado en logs de produccion). Estos headers son los
        # mismos que ya usa scripts/crawl_normativa_pdf_urls.py contra el
        # mismo dominio y ahi si pasan el WAF.
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
                "Referer": "https://www.digemid.minsa.gob.pe/",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        self.ignored_link_connection_errors = 0
        self.max_retries_429 = 3

    def fetch_pending_documents(self) -> list[dict]:
        response = (
            self.supabase
            .table(self.table_name)
            .select("id, document_key, title, detail_url, file_url, raw, process_status")
            .eq("source_type", "normativa")
            .in_("process_status", ["registered", "pdf_detection_error"])
            .not_.is_("detail_url", "null")
            .order("updated_at", desc=True)
            .execute()
        )
        return response.data or []

    def head_content_type(self, url: str) -> str:
        try:
            response = self.session.head(
                url,
                timeout=6,
                allow_redirects=True,
            )
            content_type = response.headers.get("Content-Type", "")
            if content_type:
                return content_type.lower()
        except Exception:
            return ""

        return ""

    def is_pdf_response(self, url: str) -> bool:
        if is_pdf_url(url):
            return True
        return "application/pdf" in self.head_content_type(url)

    def fetch_detail_response(self, url: str) -> requests.Response:
        response = None
        for attempt in range(1, self.max_retries_429 + 1):
            response = self.session.get(
                url,
                timeout=20,
                allow_redirects=True,
            )
            if response.status_code == 429 and attempt < self.max_retries_429:
                wait_seconds = float(response.headers.get("Retry-After", 10 * attempt))
                logger.warning(
                    "429 en %s (intento %s/%s). Espero %.1fs.",
                    url, attempt, self.max_retries_429, wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            break

        response.raise_for_status()
        return response

    def score_pdf_link(self, absolute_url: str, anchor_text: str) -> int:
        score = 0
        lowered_text = clean_text(anchor_text).lower()
        lowered_url = absolute_url.lower()

        if is_pdf_url(absolute_url):
            score += 10

        if any(hint in lowered_url for hint in PDF_PATH_HINTS):
            score += 6

        for hint in PDF_ANCHOR_HINTS:
            if hint in lowered_text:
                score += 4

        if lowered_text == "pdf":
            score += 2

        return score

    def maybe_validate_pdf_link(self, absolute_url: str, anchor_text: str) -> bool:
        lowered_text = clean_text(anchor_text).lower()
        if is_pdf_url(absolute_url):
            return True

        if not any(hint in lowered_text for hint in PDF_ANCHOR_HINTS):
            return False

        try:
            return "application/pdf" in self.head_content_type(absolute_url)
        except requests.RequestException:
            self.ignored_link_connection_errors += 1
            return False
        except Exception:
            self.ignored_link_connection_errors += 1
            return False

    def _enlaces_candidatos(self, detail_url: str, html: str) -> list[tuple[str, str]]:
        """Todos los enlaces plausibles a PDF de la pagina, con su texto de
        enlace.

        El score SOLO filtra basura evidente (score<=0) y ordena cual se
        inspecciona primero si hay que recortar por el techo de seguridad.
        NUNCA decide cual es el PDF correcto: eso lo hace
        politica_documental.decidir() tras abrir cada uno y leer su contenido.
        """
        soup = BeautifulSoup(html, "html.parser")
        vistos: dict[str, tuple[int, str]] = {}

        for anchor in soup.find_all("a", href=True):
            href = clean_text(anchor.get("href", ""))
            if not href or href.lower().startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            absolute_url = urljoin(detail_url, href)
            anchor_text = clean_text(anchor.get_text(" "))
            score = self.score_pdf_link(absolute_url, anchor_text)
            if score <= 0:
                continue

            es_pdf_directo = is_pdf_url(absolute_url) or any(
                hint in absolute_url.lower() for hint in PDF_PATH_HINTS
            )
            if not es_pdf_directo:
                try:
                    if not self.maybe_validate_pdf_link(absolute_url, anchor_text):
                        continue
                except requests.RequestException:
                    self.ignored_link_connection_errors += 1
                    continue
                except Exception:
                    self.ignored_link_connection_errors += 1
                    continue

            previo = vistos.get(absolute_url)
            if previo is None or score > previo[0]:
                vistos[absolute_url] = (score, anchor_text)

        ordenados = sorted(vistos.items(), key=lambda item: item[1][0], reverse=True)
        return [(url, texto) for url, (_score, texto) in ordenados]

    def _evidencia_de_candidato(self, url: str, anchor_text: str, identidad_objetivo) -> EvidenciaDocumental:
        """Descarga el PDF y lee sus encabezados.

        Mismo criterio que el crawler (scripts/crawl_normativa_pdf_urls.py):
        el CONTENIDO manda; el nombre del archivo y el texto del enlace solo
        acompañan.
        """
        ev = EvidenciaDocumental(
            identidad_objetivo=identidad_objetivo,
            filename=url.rsplit("/", 1)[-1],
            anchor_text=anchor_text,
            url=url,
        )
        try:
            import fitz

            respuesta = self.session.get(url, timeout=90, allow_redirects=True)
            respuesta.raise_for_status()
            ev.pdf_sha256 = hashlib.sha256(respuesta.content).hexdigest()

            with fitz.open(stream=io.BytesIO(respuesta.content), filetype="pdf") as doc:
                ev.total_paginas = doc.page_count
                textos, leidas = [], 0
                arranque = time.monotonic()
                # TODAS las paginas: el encabezado puede estar en cualquiera.
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

    def detect_pdf_url(self, detail_url: str, identidad_objetivo) -> dict:
        """Detecta y VERIFICA el pdf_url de una norma.

        Sustituye a `candidate_links[0]`: escribe solo cuando
        politica_documental.decidir() prueba, abriendo cada candidato y
        leyendo su contenido, que ese documento es la norma objetivo.
        """
        if identidad_objetivo is None:
            return {
                "status": "pdf_identity_unknown",
                "pdf_url": None,
                "mime_type": None,
                "message": (
                    "no se pudo construir una identidad objetivo verificable a "
                    "partir del titulo/document_key: sin identidad no hay nada "
                    "que comprobar, y sin comprobacion no se escribe"
                ),
            }

        if self.is_pdf_response(detail_url):
            candidatos_urls = [(detail_url, "")]
        else:
            response = self.fetch_detail_response(detail_url)
            content_type = response.headers.get("Content-Type", "").lower()
            if "application/pdf" in content_type:
                candidatos_urls = [(detail_url, "")]
            else:
                candidatos_urls = self._enlaces_candidatos(detail_url, response.text)

        if not candidatos_urls:
            return {
                "status": "pdf_not_found",
                "pdf_url": None,
                "mime_type": None,
                "message": "No se detecto enlace PDF en detail_url",
            }

        omitidos = max(0, len(candidatos_urls) - MAX_CANDIDATOS)
        candidatos_urls = candidatos_urls[:MAX_CANDIDATOS]

        evidencias = [
            self._evidencia_de_candidato(url, anchor_text, identidad_objetivo)
            for url, anchor_text in candidatos_urls
        ]

        decision = decidir(
            evidencias, identidad_objetivo,
            candidatos_omitidos=omitidos,
            motivo_omision=(f"detail_url listaba mas de {MAX_CANDIDATOS} PDF" if omitidos else ""),
        )

        candidatos_evaluados = [
            c.get("url") for c in (decision.evidencia or {}).get("candidatos", [])
        ]

        if decision.escribir:
            return {
                "status": "pdf_detected",
                "pdf_url": decision.url,
                "mime_type": "application/pdf",
                "message": decision.motivo,
                "candidatos": candidatos_evaluados,
            }

        return {
            "status": _ESTADO_A_STATUS.get(decision.estado, "pdf_ambiguous"),
            "pdf_url": None,
            "mime_type": None,
            "message": decision.motivo,
            "candidatos": candidatos_evaluados,
        }

    def update_document(self, row: dict, result: dict) -> None:
        now = utc_now_iso()
        raw = dict(row.get("raw") or {})
        raw["pdf_detection"] = {
            "status": result["status"],
            "detail_url": row.get("detail_url"),
            "pdf_url": result.get("pdf_url"),
            "detected_at": now,
            "message": result.get("message"),
            "mime_type_detectado": result.get("mime_type"),
        }

        if result.get("candidatos"):
            raw["pdf_detection"]["candidatos"] = result["candidatos"]

        payload = {
            "has_file": result["status"] == "pdf_detected",
            "process_status": result["status"],
            "process_message": result["message"],
            "updated_at": now,
            "raw": raw,
        }

        if result["status"] == "pdf_detected":
            payload.update(
                {
                    "file_url": result["pdf_url"],
                    "file_name": extract_file_name(result["pdf_url"]),
                    "file_ext": "pdf",
                    "mime_type": result.get("mime_type") or "application/pdf",
                }
            )

        (
            self.supabase
            .table(self.table_name)
            .update(payload)
            .eq("id", row["id"])
            .execute()
        )

    def process(self) -> dict:
        rows = self.fetch_pending_documents()
        summary = {
            "total_pending": len(rows),
            "pdf_detected": 0,
            "pdf_not_found": 0,
            # F-03B: ningun candidato prueba, por contenido, ser la norma
            # objetivo. NO se reintenta solo -no hay nada que reintentar-:
            # queda para un humano, con los candidatos anotados en
            # raw.pdf_detection.candidatos.
            "pdf_ambiguous": 0,
            # La pagina listaba mas PDF de los que el techo de seguridad
            # permite inspeccionar. "no los mire todos" NUNCA se degrada a
            # "la norma no esta en ninguno".
            "pdf_audit_incomplete": 0,
            # No se pudo construir una identidad objetivo verificable a partir
            # del titulo/document_key: sin identidad no hay nada que probar.
            "pdf_identity_unknown": 0,
            "pdf_detection_error": 0,
            "ignored_link_connection_errors": 0,
        }

        logger.info("total_pending=%s", summary["total_pending"])

        for row in rows:
            document_key = row.get("document_key")
            detail_url = row.get("detail_url")
            identidad_objetivo = identidad_objetivo_de_documento(
                row.get("title"), document_key
            )

            try:
                logger.info("Detectando PDF normativo: %s | %s", document_key, detail_url)
                result = self.detect_pdf_url(detail_url, identidad_objetivo)
                self.update_document(row, result)
                summary[result["status"]] = summary.get(result["status"], 0) + 1
            except Exception as error:
                now = utc_now_iso()
                raw = dict(row.get("raw") or {})
                raw["pdf_detection"] = {
                    "status": "pdf_detection_error",
                    "detail_url": detail_url,
                    "detected_at": now,
                    "message": str(error)[:300],
                }

                (
                    self.supabase
                    .table(self.table_name)
                    .update(
                        {
                            "has_file": False,
                            "process_status": "pdf_detection_error",
                            "process_message": str(error)[:300],
                            "updated_at": now,
                            "raw": raw,
                        }
                    )
                    .eq("id", row["id"])
                    .execute()
                )

                summary["pdf_detection_error"] += 1
                logger.exception(
                    "Error detectando PDF normativo %s: %s",
                    document_key,
                    error,
                )

            time.sleep(random.uniform(0.8, 1.5))

        summary["ignored_link_connection_errors"] = self.ignored_link_connection_errors

        logger.info(
            "Resumen deteccion PDF | total_pending=%s | pdf_detected=%s | pdf_not_found=%s | "
            "pdf_ambiguous=%s | pdf_audit_incomplete=%s | pdf_identity_unknown=%s | "
            "pdf_detection_error=%s | ignored_link_connection_errors=%s",
            summary["total_pending"],
            summary["pdf_detected"],
            summary["pdf_not_found"],
            summary["pdf_ambiguous"],
            summary["pdf_audit_incomplete"],
            summary["pdf_identity_unknown"],
            summary["pdf_detection_error"],
            summary["ignored_link_connection_errors"],
        )
        return summary
