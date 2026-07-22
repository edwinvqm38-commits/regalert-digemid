import logging
import os
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from agents.agent_utils import (
    clean_text,
    extract_date_display,
    generate_document_key,
    is_valid_document,
    normalize_date,
    slug_from_url,
    utc_now_iso,
)

logger = logging.getLogger(__name__)


class MonitorAgent:
    """Agente de scraping controlado para detectar alertas DIGEMID."""

    def __init__(self):
        self.source_url = os.getenv(
            "DIGEMID_SOURCE_URL",
            "https://www.digemid.minsa.gob.pe/webDigemid/alertas-modificaciones/",
        )
        self.headers = {
            "User-Agent": "RegAlert-DIGEMID-Monitor/1.0"
        }

    def fetch_html(self) -> str:
        """Descarga el HTML de la página fuente."""
        logger.info("Consultando fuente DIGEMID: %s", self.source_url)

        response = requests.get(
            self.source_url,
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()

        return response.text

    def get_latest_alerts(self) -> list[dict]:
        """Consulta DIGEMID y devuelve documentos normalizados."""
        try:
            html = self.fetch_html()
            soup = BeautifulSoup(html, "html.parser")

            documents: list[dict] = []
            candidate_count = 0
            rejected_sample: list[str] = []

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                text = clean_text(link.get_text(" "))

                detail_url = urljoin(self.source_url, href)
                detail_url_lower = detail_url.lower()
                text_lower = text.lower()

                is_alert_link = (
                    "alerta-digemid" in detail_url_lower
                    or "alerta digemid" in text_lower
                    or "alerta digemid" in detail_url_lower.replace("-", " ")
                )

                # Red mas amplia: cualquier link bajo /alertas-modificaciones/<anio>/...
                # que no haya calzado con el filtro estricto de arriba. Sirve para
                # detectar en los logs si DIGEMID cambio el formato de sus URLs/textos
                # y el filtro estricto empezo a descartar alertas reales en silencio.
                looks_like_alert_path = bool(
                    re.search(r"/alertas-modificaciones/20\d{2}/[^/]+/?$", detail_url_lower)
                )

                if looks_like_alert_path:
                    candidate_count += 1

                if not is_alert_link:
                    if looks_like_alert_path and len(rejected_sample) < 20:
                        rejected_sample.append(f"href={detail_url!r} text={text!r}")
                    continue

                document_key = generate_document_key(text, detail_url)

                if not document_key:
                    continue

                date_display = extract_date_display(text)
                published_date = normalize_date(date_display)

                title = text or f"Alerta DIGEMID {document_key}"
                doc_slug = slug_from_url(detail_url)

                doc = {
                    "source_type": "alerta",
                    "source_section": "alertas-modificaciones",
                    "document_key": document_key,
                    "title": title,
                    "document_slug": doc_slug,
                    "detail_url": detail_url,
                    "published_date": published_date,
                    "published_date_display": date_display,
                    "has_file": False,
                    "process_status": "pendiente",
                    "raw": {
                        "source": "github_actions_python",
                        "source_url": self.source_url,
                        "scraped_at": utc_now_iso(),
                    },
                }

                if is_valid_document(doc):
                    documents.append(doc)

            unique_docs = {}

            for doc in documents:
                unique_docs[doc["document_key"]] = doc

            result = list(unique_docs.values())

            logger.info("Documentos detectados por MonitorAgent: %s", len(result))

            if rejected_sample:
                logger.warning(
                    "DIAGNOSTICO: %s link(s) con patron /alertas-modificaciones/<anio>/... "
                    "no calzaron con el filtro estricto is_alert_link (posible cambio de "
                    "formato en DIGEMID). Muestra: %s",
                    candidate_count,
                    rejected_sample,
                )

            return result

        except Exception as error:
            logger.exception("Error en MonitorAgent: %s", error)
            return []